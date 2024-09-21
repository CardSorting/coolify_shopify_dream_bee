import asyncio
import uuid
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import discord
from discord import app_commands, Interaction, InteractionType
from discord.ext import commands, tasks

from handlers.flux_image_handler import FluxImageHandler
from handlers.backblaze_handler import BackblazeHandler
from handlers.product_handler import ProductHandler
from utils.embed_creator import EmbedCreator
from utils.logger import Logger
from utils.in_memory_queue import InMemoryQueue, QueueFullError, QueueEmptyError


@dataclass
class CachedInteraction:
    interaction: Interaction
    channel_id: int
    user_id: int
    message_id: Optional[int] = None
    guild_id: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    prompt: Optional[str] = None  # Added prompt field


class ImageProductCommand(commands.Cog):
    """A Discord Cog for handling image generation and product creation."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.logger = Logger.get_instance(self.__class__.__name__)

        # Initialize handlers
        self.flux_handler = FluxImageHandler()
        self.backblaze_handler = BackblazeHandler(
            key_id=bot.BACKBLAZE_KEY_ID,
            application_key=bot.BACKBLAZE_APPLICATION_KEY,
            bucket_name=bot.BACKBLAZE_BUCKET_NAME
        )
        self.product_handler = ProductHandler(
            admin_api_token=bot.SHOPIFY_ADMIN_API_TOKEN,
            shop_name=bot.SHOPIFY_SHOP_NAME,
            backblaze_handler=self.backblaze_handler
        )
        self.embed_creator = EmbedCreator()

        # Initialize queues
        self.image_generation_queue = InMemoryQueue[Dict[str, Any]](
            max_size=50, name="image_generation_queue"
        )
        self.product_creation_queue = InMemoryQueue[Dict[str, Any]](
            max_size=100, name="product_creation_queue"
        )

        # In-memory cache for image URLs
        self.image_url_cache: Dict[str, str] = {}

        # Interaction cache to store CachedInteraction objects for follow-up
        self.interaction_cache: Dict[int, CachedInteraction] = {}
        self.interaction_cache_lock = asyncio.Lock()

        # Start queue processing tasks
        self.process_image_generation_queue.start()
        self.process_product_creation_queue.start()

        # Start a background task to clean up old interactions
        self.cleanup_task = self.bot.loop.create_task(self._cleanup_interaction_cache())

    def cog_unload(self):
        """Clean up resources when the cog is unloaded."""
        self.process_image_generation_queue.cancel()
        self.process_product_creation_queue.cancel()
        self.cleanup_task.cancel()
        asyncio.create_task(self.image_generation_queue.close())
        asyncio.create_task(self.product_creation_queue.close())

    async def _cleanup_interaction_cache(self):
        """Periodically clean up the interaction cache to prevent memory leaks."""
        while True:
            await asyncio.sleep(900)  # Run every 15 minutes
            async with self.interaction_cache_lock:
                cutoff = datetime.utcnow() - timedelta(minutes=15)
                to_remove = [
                    id_ for id_, cached in self.interaction_cache.items()
                    if cached.timestamp < cutoff
                ]
                for id_ in to_remove:
                    del self.interaction_cache[id_]
                self.logger.info(f"Interaction cache cleaned. Removed {len(to_remove)} interactions.")

    @app_commands.command(
        name='generate',
        description="Generate a product image using AI"
    )
    @app_commands.describe(prompt="The prompt to generate the image with")
    async def generate_product(self, interaction: discord.Interaction, prompt: str):
        """Command to generate a product image using AI."""
        await interaction.response.defer(ephemeral=True)
        try:
            # Cache the interaction along with channel_id, user_id, and prompt for future follow-ups
            cached_interaction = CachedInteraction(
                interaction=interaction,
                channel_id=interaction.channel_id,
                user_id=interaction.user.id,
                guild_id=interaction.guild_id,
                prompt=prompt  # Store the prompt
            )
            async with self.interaction_cache_lock:
                self.interaction_cache[interaction.id] = cached_interaction

            await self.image_generation_queue.enqueue({
                'interaction_id': interaction.id,
                'prompt': prompt
            })
            await interaction.followup.send(
                "Your image generation request has been queued. We'll notify you once it's ready.",
                ephemeral=True
            )
        except QueueFullError:
            await interaction.followup.send(
                "We're currently processing too many requests. Please try again later.",
                ephemeral=True
            )
        except discord.Forbidden:
            self.logger.error("Failed to send follow-up message due to lack of permissions.")
            await interaction.followup.send(
                "I don't have permission to send messages in this channel. Please check my permissions.",
                ephemeral=True
            )
        except Exception as e:
            self.logger.error(f"Error in generate_product: {e}", exc_info=True)
            await interaction.followup.send(
                "An unexpected error occurred. Please try again or contact support if the problem persists.",
                ephemeral=True
            )

    @tasks.loop(seconds=1.0)
    async def process_image_generation_queue(self):
        """Process items in the image generation queue."""
        try:
            item = await self.image_generation_queue.dequeue()
            await self._process_image_generation(item)
        except QueueEmptyError:
            await asyncio.sleep(0.1)  # Short sleep to prevent tight loop
        except Exception as e:
            self.logger.error(f"Error processing image generation queue: {e}", exc_info=True)
            await asyncio.sleep(1)  # Longer sleep on error to prevent rapid retries

    async def _process_image_generation(self, item: Dict[str, Any]):
        """Process a single image generation request."""
        interaction_id = item['interaction_id']
        prompt = item['prompt']

        try:
            image_url = await self._generate_and_upload_image(prompt)
            if not image_url:
                await self._send_followup(
                    interaction_id,
                    "Failed to generate or upload the image. Please try again."
                )
                return

            embed = self.embed_creator.create_image_embed(image_url, prompt)
            view = await self._create_product_options_view(image_url, interaction_id)
            await self._send_followup(interaction_id, embed=embed, view=view)
        except Exception as e:
            self.logger.error(f"Error in _process_image_generation: {e}", exc_info=True)
            await self._send_followup(
                interaction_id,
                "An unexpected error occurred. Please try again later."
            )

    async def _generate_and_upload_image(self, prompt: str) -> Optional[str]:
        """Generate an image and upload it to Backblaze."""
        try:
            image_url = await self.flux_handler.generate_image(prompt)
            if not image_url:
                self.logger.warning("Image generation failed: No URL returned.")
                return None

            image_content = await self.flux_handler.download_image(image_url)
            if not image_content:
                self.logger.warning("Image download failed: No content retrieved.")
                return None

            file_name = f"ATC_{uuid.uuid4()}.jpg"
            backblaze_url = await self.backblaze_handler.upload_image(file_name, image_content)
            if not backblaze_url:
                self.logger.warning("Image upload failed: No URL returned from Backblaze.")
            return backblaze_url
        except Exception as e:
            self.logger.error(f"Error in _generate_and_upload_image: {e}", exc_info=True)
            return None

    async def _create_product_options_view(
            self, image_url: str, interaction_id: int) -> discord.ui.View:
        """Create a view with a button to add the product to Shopify."""
        view = discord.ui.View(timeout=None)
        short_id = str(uuid.uuid4())[:8]
        self.image_url_cache[short_id] = image_url
        button = discord.ui.Button(
            label="Add to Shop",
            style=discord.ButtonStyle.green,
            custom_id=f"add_to_shopify|{short_id}|{interaction_id}"
        )
        view.add_item(button)
        return view

    async def handle_add_to_shopify(self, interaction: Interaction):
        """Handle the 'Add to Shopify' button click."""
        if interaction.type != InteractionType.component:
            await interaction.response.send_message(
                "Invalid interaction type.", ephemeral=True)
            return

        if not isinstance(interaction.data, dict):
            await interaction.response.send_message(
                "Invalid interaction data.", ephemeral=True)
            return

        custom_id = interaction.data.get('custom_id')
        if not custom_id:
            await interaction.response.send_message(
                "Invalid interaction data.", ephemeral=True)
            return

        custom_id_parts = custom_id.split('|')
        if len(custom_id_parts) != 3:
            await interaction.response.send_message(
                "Invalid product data. Please try again.", ephemeral=True)
            return

        short_id, original_interaction_id = custom_id_parts[1], int(custom_id_parts[2])
        image_url = self.image_url_cache.get(short_id)

        # Retrieve the prompt from the cached interaction
        prompt = None
        async with self.interaction_cache_lock:
            cached_interaction = self.interaction_cache.get(original_interaction_id)
            if cached_interaction:
                prompt = cached_interaction.prompt

        if not image_url or not prompt:
            await interaction.response.send_message(
                "Product data or prompt not found. Please try generating the image again.",
                ephemeral=True)
            return

        try:
            # Defer the interaction to acknowledge it and defer the response
            await interaction.response.defer(ephemeral=True)

            await self.product_creation_queue.enqueue({
                'interaction_id': interaction.id,
                'original_interaction_id': original_interaction_id,
                'image_url': image_url,
                'user_id': interaction.user.id,
                'username': interaction.user.name,  # Corrected to use 'name' attribute
                'prompt': prompt  # Pass the prompt to the queue
            })
            await interaction.followup.send(
                "Your product creation request has been queued. We'll notify you once it's processed.",
                ephemeral=True
            )
        except QueueFullError:
            await interaction.followup.send(
                "We're currently processing too many requests. Please try again later.",
                ephemeral=True
            )
        except discord.Forbidden:
            self.logger.error("Failed to send follow-up message due to lack of permissions.")
            await interaction.followup.send(
                "I don't have permission to send messages in this channel. Please check my permissions.",
                ephemeral=True
            )
        except Exception as e:
            self.logger.error(f"Error in handle_add_to_shopify: {e}", exc_info=True)
            await interaction.followup.send(
                "An unexpected error occurred. Please try again or contact support if the problem persists.",
                ephemeral=True
            )

    @tasks.loop(seconds=1.0)
    async def process_product_creation_queue(self):
        """Process items in the product creation queue."""
        try:
            item = await self.product_creation_queue.dequeue()
            await self._process_product_creation(item)
        except QueueEmptyError:
            await asyncio.sleep(0.1)  # Short sleep to prevent tight loop
        except Exception as e:
            self.logger.error(f"Error processing product creation queue: {e}", exc_info=True)
            await asyncio.sleep(1)  # Longer sleep on error to prevent rapid retries

    async def _process_product_creation(self, item: Dict[str, Any]):
        """Process a single product creation request."""
        interaction_id = item['interaction_id']
        original_interaction_id = item['original_interaction_id']
        image_url = item['image_url']
        user_id = item['user_id']
        username = item['username']
        prompt = item['prompt']

        try:
            product_data = self._create_product_data(username, image_url, prompt)
            response = await self.product_handler.add_product_to_shopify(product_data)

            if response and 'product_url' in response:
                embed = self._create_product_confirmation_embed(
                    product_data, response['product_url']
                )
                await self._send_followup(original_interaction_id, embed=embed)
            else:
                await self._send_followup(
                    original_interaction_id,
                    "Failed to add product to Shopify. Please try again."
                )
        except Exception as e:
            self.logger.error(f"Error in _process_product_creation: {e}", exc_info=True)
            await self._send_followup(
                original_interaction_id,
                "An unexpected error occurred while creating the product. Please try again later."
            )

    def _create_product_data(self, username: str, image_url: str, prompt: str) -> Dict[str, Any]:
        """Create product data for Shopify with a unique, human-readable title.

        Args:
            username (str): The username of the user creating the product.
            image_url (str): The URL of the generated image.
            prompt (str): The prompt used to generate the image.

        Returns:
            Dict[str, Any]: A dictionary containing product data for Shopify.
        """
        # Extract the first two words from the prompt
        prompt_words = prompt.strip().split()
        if len(prompt_words) >= 2:
            first_two_words = ' '.join(prompt_words[:2]).title()
        elif len(prompt_words) == 1:
            first_two_words = prompt_words[0].title()
        else:
            first_two_words = "Unique"

        # Construct the title
        title = f"{first_two_words} Artist Trading Card (ATC) by {username}"

        return {
            "title": title,
            "description": self._get_product_description(),
            "image_url": image_url,
            "vendor": username,  # Use username for vendor
            "price": 6.99
        }

    def _create_product_confirmation_embed(
            self, product_data: Dict[str, Any], product_url: str) -> discord.Embed:
        """Create an embed for product confirmation."""
        return self.embed_creator.create_confirmation_embed(
            title="Product Added Successfully",
            description=(
                f"Product '{product_data['title']}' has been added to Shopify "
                f"with a price of ${product_data['price']:.2f}.\n\n"
                f"View product: {product_url}"
            )
        )

    async def _send_followup(
            self,
            interaction_id: int,
            content: Optional[str] = None,
            **kwargs: Any):
        """
        Send a follow-up message to a user based on the interaction ID.

        The method attempts the following in order:
        1. Sends a follow-up using the original interaction.
        2. Sends a message to the channel where the interaction occurred.
        3. Sends a direct message to the user.
        4. Logs an error if all attempts fail.
        """
        try:
            async with self.interaction_cache_lock:
                cached_interaction: Optional[CachedInteraction] = self.interaction_cache.get(interaction_id)

            if cached_interaction:
                # Step 1: Attempt to send follow-up using the original interaction
                try:
                    await cached_interaction.interaction.followup.send(content, **kwargs)
                    self.logger.info(f"Follow-up sent via original interaction {interaction_id}.")
                    return
                except discord.DiscordException as e:
                    self.logger.warning(
                        f"Failed to send follow-up via original interaction {interaction_id}: {e}"
                    )

            # Step 2: Attempt to send to the original channel
            if cached_interaction and cached_interaction.channel_id:
                channel = self.bot.get_channel(cached_interaction.channel_id)
                if channel and isinstance(channel, discord.TextChannel):
                    permissions = channel.permissions_for(channel.guild.me)
                    if permissions.send_messages:
                        try:
                            sent_message = await channel.send(content, **kwargs)
                            self.logger.info(
                                f"Follow-up sent to channel {channel.id} for interaction {interaction_id}."
                            )
                            if cached_interaction:
                                cached_interaction.message_id = sent_message.id
                            return
                        except discord.Forbidden:
                            self.logger.error(f"Permission denied: Cannot send messages to channel {channel.id}.")
                        except discord.HTTPException as e:
                            self.logger.error(f"HTTPException when sending to channel {channel.id}: {e}")
                    else:
                        self.logger.error(f"Insufficient permissions to send messages to channel {channel.id}.")
                else:
                    self.logger.warning(
                        f"Channel {cached_interaction.channel_id} not found or is not a TextChannel."
                    )

            # Step 3: Attempt to send a DM to the user
            if cached_interaction and cached_interaction.user_id:
                try:
                    user = await self.bot.fetch_user(cached_interaction.user_id)
                    if user:
                        if user.dm_channel is None:
                            await user.create_dm()
                        try:
                            await user.send(content, **kwargs)
                            self.logger.info(
                                f"Follow-up sent via DM to user {user.id} for interaction {interaction_id}."
                            )
                            return
                        except discord.Forbidden:
                            self.logger.error(
                                f"Cannot send DM to user {user.id}. They might have DMs disabled."
                            )
                        except discord.HTTPException as e:
                            self.logger.error(
                                f"HTTPException when sending DM to user {user.id}: {e}"
                            )
                except discord.NotFound:
                    self.logger.error(f"User with ID {cached_interaction.user_id} not found.")
                except discord.HTTPException as e:
                    self.logger.error(f"HTTPException when fetching user {cached_interaction.user_id}: {e}")

            # Step 4: Log an error if all attempts fail
            self.logger.error(
                f"Unable to send follow-up message for interaction {interaction_id}."
            )

        except Exception as e:
            self.logger.error(
                f"Unexpected error in _send_followup for interaction {interaction_id}: {e}",
                exc_info=True
            )

    def _get_product_description(self) -> str:
        """Get the default product description."""
        return (
            "Artist Trading Card (ATC) – 2.5 x 3.5 inches\n\n"
            "Discover the charm and creativity of artist trading cards, each meticulously "
            "crafted to a precise 2.5 x 3.5 inches. Perfect for art enthusiasts, collectors, "
            "and creators alike, these miniature canvases offer endless possibilities for "
            "artistic expression.\n\n"
            "Celebrate the art of small-scale creativity with these artist trading cards, where "
            "every inch is an opportunity for a masterpiece. Perfect for any art lover looking to "
            "expand their collection or add a unique personal touch to their projects."
        )

    @commands.Cog.listener()
    async def on_ready(self):
        """Event listener for when the cog is ready."""
        self.logger.info(f"{self.__class__.__name__} Cog is ready.")

    @app_commands.command(
        name='queue_stats',
        description="View statistics for the image generation and product creation queues"
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def queue_stats(self, interaction: discord.Interaction):
        """Command to view queue statistics (admin only)."""
        try:
            image_queue_stats = await self.image_generation_queue.get_statistics()
            product_queue_stats = await self.product_creation_queue.get_statistics()

            stats_message = self._format_queue_stats(image_queue_stats, product_queue_stats)
            await interaction.response.send_message(stats_message, ephemeral=True)
        except Exception as e:
            self.logger.error(f"Error in queue_stats command: {e}", exc_info=True)
            await interaction.response.send_message(
                "An error occurred while fetching queue statistics.",
                ephemeral=True
            )

    def _format_queue_stats(
            self,
            image_stats: Dict[str, Any],
            product_stats: Dict[str, Any]
    ) -> str:
        """Format queue statistics into a readable message."""
        return (
            f"**Image Generation Queue:**\n"
            f"- Current size: {image_stats['current_size']}\n"
            f"- Total enqueued: {image_stats['total_enqueued']}\n"
            f"- Total dequeued: {image_stats['total_dequeued']}\n\n"
            f"**Product Creation Queue:**\n"
            f"- Current size: {product_stats['current_size']}\n"
            f"- Total enqueued: {product_stats['total_enqueued']}\n"
            f"- Total dequeued: {product_stats['total_dequeued']}"
        )

    @commands.Cog.listener()
    async def on_interaction(self, interaction: Interaction):
        """
        Listen for interactions to handle component interactions like buttons.

        This is necessary to route button clicks to their respective handlers.
        """
        if interaction.type == InteractionType.component:
            custom_id = interaction.data.get('custom_id', '')
            if custom_id.startswith('add_to_shopify'):
                await self.handle_add_to_shopify(interaction)


async def setup(bot: commands.Bot):
    """Setup function to add the cog to the bot."""
    await bot.add_cog(ImageProductCommand(bot))