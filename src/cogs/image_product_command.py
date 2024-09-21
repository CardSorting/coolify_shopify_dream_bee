# cogs/image_product_command.py
import asyncio
import uuid
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import discord
from discord import app_commands, Interaction
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
        user_id = interaction.user.id

        # Check if the user has at least 1 credit
        credits = await self.bot.credit_system.get_credits(user_id)
        if credits < 1:
            await interaction.followup.send(
                "You do not have enough credits to generate an image. Please claim your daily credits using `!claim`.",
                ephemeral=True
            )
            return

        # Deduct 1 credit
        success = await self.bot.credit_system.deduct_credit(user_id, 1)
        if not success:
            await interaction.followup.send(
                "Failed to deduct credit. Please try again.",
                ephemeral=True
            )
            return

        # Inform the user about remaining credits
        remaining_credits = await self.bot.credit_system.get_credits(user_id)
        await interaction.followup.send(
            f"1 credit has been deducted for generating your image. You have **{remaining_credits}** credits remaining.",
            ephemeral=True
        )

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

            # Create an embed with the image and prompt
            embed = self.embed_creator.create_image_embed(image_url, prompt)
            await self._send_followup(interaction_id, embed=embed)

            # Enqueue product creation automatically
            async with self.interaction_cache_lock:
                cached_interaction = self.interaction_cache.get(interaction_id)
                if not cached_interaction:
                    self.logger.error(f"No cached interaction found for ID {interaction_id}")
                    return

                await self.product_creation_queue.enqueue({
                    'interaction_id': interaction_id,
                    'image_url': image_url,
                    'user_id': cached_interaction.user_id,
                    'username': cached_interaction.interaction.user.name,
                    'prompt': prompt
                })

            self.logger.info(f"Image generated and product creation enqueued for interaction {interaction_id}.")

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
            else:
                self.logger.info(f"Image '{file_name}' uploaded successfully to Backblaze. URL: {backblaze_url}")
            return backblaze_url
        except Exception as e:
            self.logger.error(f"Error in _generate_and_upload_image: {e}", exc_info=True)
            return None

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
        image_url = item['image_url']
        user_id = item['user_id']
        username = item['username']
        prompt = item['prompt']

        try:
            product_data = self._create_product_data(username, image_url, prompt)
            response = await self.product_handler.add_product_to_shopify(product_data)

            self.logger.debug(f"Product creation response: {response}")

            # Adjust the condition based on actual response structure
            if response and 'product_url' in response:
                # Successfully added to Shopify
                self.logger.info(
                    f"Product '{product_data['title']}' created successfully for interaction {interaction_id}."
                )
            else:
                # Failed to add to Shopify
                self.logger.error(
                    f"Failed to add product '{product_data['title']}' to Shopify for interaction {interaction_id}."
                )
        except Exception as e:
            self.logger.error(f"Error in _process_product_creation: {e}", exc_info=True)
        finally:
            # Remove interaction from cache to prevent memory leaks
            async with self.interaction_cache_lock:
                if interaction_id in self.interaction_cache:
                    del self.interaction_cache[interaction_id]
                    self.logger.debug(f"Removed interaction {interaction_id} from cache after processing.")

    def _create_product_data(self, username: str, image_url: str, prompt: str) -> Dict[str, Any]:
        """Create product data for Shopify with a unique, human-readable title.

        Args:
            username (str): The username of the user creating the product.
            image_url (str): The URL of the generated image.
            prompt (str): The prompt used to generate the image.

        Returns:
            Dict[str, Any]: A dictionary containing product data for Shopify.
        """
        # Extract the first four words from the prompt
        prompt_words = prompt.strip().split()
        if len(prompt_words) >= 4:
            first_four_words = ' '.join(prompt_words[:4]).title()  # Get the first four words and title case them
        else:
            first_four_words = ' '.join(prompt_words).title()  # Title case the entire prompt if fewer than 4 words

        # Construct the title
        title = f"{first_four_words} Artist Trading Card (ATC) by {username}"

        return {
            "title": title,
            "description": self._get_product_description(),
            "image_url": image_url,
            "vendor": username,  # Use username for vendor
            "price": 6.99
        }

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
                    if 'embed' in kwargs:
                        await cached_interaction.interaction.followup.send(embed=kwargs['embed'], **{k: v for k, v in kwargs.items() if k != 'embed'})
                    else:
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


async def setup(bot: commands.Bot):
    """Setup function to add the cog to the bot."""
    await bot.add_cog(ImageProductCommand(bot))