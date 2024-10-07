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
from services.shopify_service import ShopifyService
from utils.embed_creator import EmbedCreator
from utils.logger import Logger
from utils.in_memory_queue import InMemoryQueue, QueueFullError, QueueEmptyError
from credit_system import CreditSystem

# Import your custom bot class
from main import DiscordShopifyBot  # Adjust the import path according to your project structure

@dataclass
class CachedInteraction:
    interaction: Interaction
    channel_id: int
    user_id: int
    guild_id: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    prompt: Optional[str] = None  # Stores the prompt for the interaction
    message_id: Optional[int] = None  # To track sent messages if needed

class ImageProductCommand(commands.Cog):
    """A Discord Cog for handling image generation and product creation."""

    def __init__(self, bot: DiscordShopifyBot):
        self.bot = bot
        self.logger = Logger.get_instance(self.__class__.__name__)

        # Initialize handlers
        self.flux_handler = FluxImageHandler()
        self.backblaze_handler = bot.backblaze_handler
        self.product_handler = bot.product_handler
        self.embed_creator = EmbedCreator()
        self.credit_system = bot.credit_system
        self.shopify_service = bot.shopify_service

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

        # Background tasks will be initialized in cog_load
        self.cleanup_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        """Called when the cog is loaded."""
        # Start queue processing tasks
        self.process_image_generation_queue.start()
        self.process_product_creation_queue.start()

        # Start a background task to clean up old interactions
        self.cleanup_task = asyncio.create_task(self._cleanup_interaction_cache())
        self.logger.info("ImageProductCommand cog loaded and tasks started.")

    async def cog_unload(self):
        """Clean up resources when the cog is unloaded."""
        # Cancel background tasks
        self.process_image_generation_queue.cancel()
        self.process_product_creation_queue.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()

        # Close queues
        await self.image_generation_queue.close()
        await self.product_creation_queue.close()
        self.logger.info("ImageProductCommand cog unloaded and tasks cancelled.")

    async def _cleanup_interaction_cache(self):
        """Periodically clean up the interaction cache to prevent memory leaks."""
        try:
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
        except asyncio.CancelledError:
            self.logger.info("Cleanup task has been cancelled.")
        except Exception as e:
            self.logger.error(f"Error in _cleanup_interaction_cache: {e}", exc_info=True)

    @app_commands.command(
        name='dream',
        description="Dream a beautiful dream with the help of DreamBee"
    )
    @app_commands.describe(prompt="Dream a beautiful dream with the help of DreamBee")
    async def generate_product(self, interaction: Interaction, prompt: str):
        """Dream a beautiful dream with the help of DreamBee"""
        await interaction.response.defer(ephemeral=True)
        user_id = interaction.user.id

        # Check if the user has at least 1 credit
        try:
            credits = await self.credit_system.get_credits(user_id)
            if credits < 1:
                await interaction.followup.send(
                    content="You do not have enough credits to generate an image. Please claim your daily credits using `/claim`.",
                    ephemeral=True
                )
                return
        except Exception as e:
            self.logger.error(f"Error getting credits for user {user_id}: {e}", exc_info=True)
            await interaction.followup.send(
                content="An error occurred while checking your credits. Please try again later.",
                ephemeral=True
            )
            return

        # Deduct 1 credit
        try:
            success = await self.credit_system.deduct_credit(user_id, 1)
            if not success:
                await interaction.followup.send(
                    content="Failed to deduct credit. Please try again.",
                    ephemeral=True
                )
                return
        except Exception as e:
            self.logger.error(f"Error deducting credit for user {user_id}: {e}", exc_info=True)
            await interaction.followup.send(
                content="An error occurred while deducting your credit. Please try again later.",
                ephemeral=True
            )
            return

        # Inform the user about remaining credits
        try:
            remaining_credits = await self.credit_system.get_credits(user_id)
            await interaction.followup.send(
                content=f"1 credit has been deducted for generating your image. You have **{remaining_credits}** credits remaining.",
                ephemeral=True
            )
        except Exception as e:
            self.logger.error(f"Error getting remaining credits for user {user_id}: {e}", exc_info=True)
            # Proceeding despite the error

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
                content="Your image generation request has been queued. We'll notify you once it's ready.",
                ephemeral=True
            )
        except QueueFullError:
            await interaction.followup.send(
                content="We're currently processing too many requests. Please try again later.",
                ephemeral=True
            )
        except discord.Forbidden:
            self.logger.error("Failed to send follow-up message due to lack of permissions.")
        except Exception as e:
            self.logger.error(f"Error in generate_product: {e}", exc_info=True)
            await interaction.followup.send(
                content="An unexpected error occurred. Please try again or contact support if the problem persists.",
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
        except asyncio.CancelledError:
            self.logger.info("Image generation queue processing task has been cancelled.")
        except Exception as e:
            self.logger.error(f"Error processing image generation queue: {e}", exc_info=True)
            await asyncio.sleep(1)  # Longer sleep on error to prevent rapid retries

    async def _process_image_generation(self, item: Dict[str, Any]):
        """Process a single image generation request."""
        interaction_id = item['interaction_id']
        prompt = item['prompt']

        try:
            # Generate and upload the image
            image_url = await self._generate_and_upload_image(prompt)
            if not image_url:
                await self._send_followup(
                    interaction_id,
                    content="Failed to generate or upload the image. Please try again."
                )
                return

            # Create an embed with the image and prompt
            embed = self.embed_creator.create_image_embed(image_url, prompt)
            await self._send_followup(interaction_id, content=None, embed=embed)

            # Enqueue product creation automatically
            async with self.interaction_cache_lock:
                cached_interaction = self.interaction_cache.get(interaction_id)
                if not cached_interaction:
                    self.logger.error(f"No cached interaction found for ID {interaction_id}")
                    return

                product_data = {
                    "title": self._create_product_title(prompt, cached_interaction.interaction.user.name),
                    "body_html": self._get_product_description(),
                    "image_url": image_url,
                    "vendor": cached_interaction.interaction.user.name,
                    "variants": [
                        {
                            "price": "6.99"
                        }
                    ],
                    "tags": [f"Artist-{cached_interaction.interaction.user.name}"]
                }

                await self.product_creation_queue.enqueue({
                    'interaction_id': interaction_id,
                    'product_data': product_data,
                    'username': cached_interaction.interaction.user.name
                })

            self.logger.info(f"Image generated and product creation enqueued for interaction {interaction_id}.")

        except Exception as e:
            self.logger.error(f"Error in _process_image_generation: {e}", exc_info=True)
            await self._send_followup(
                interaction_id,
                content="An unexpected error occurred. Please try again later."
            )

    @tasks.loop(seconds=1.0)
    async def process_product_creation_queue(self):
        """Process items in the product creation queue."""
        try:
            item = await self.product_creation_queue.dequeue()
            await self._process_product_creation(item)
        except QueueEmptyError:
            await asyncio.sleep(0.1)  # Short sleep to prevent tight loop
        except asyncio.CancelledError:
            self.logger.info("Product creation queue processing task has been cancelled.")
        except Exception as e:
            self.logger.error(f"Error processing product creation queue: {e}", exc_info=True)
            await asyncio.sleep(1)  # Longer sleep on error to prevent rapid retries

    async def _process_product_creation(self, item: Dict[str, Any]):
        """Process a single product creation request."""
        interaction_id = item['interaction_id']
        product_data = item['product_data']
        username = item['username']

        try:
            # Create the product in Shopify
            response = await self.product_handler.add_product_to_shopify(product_data, username)

            self.logger.debug(f"Product creation response: {response}")

            if response and 'product' in response:
                product = response['product']

        finally:
            # Remove interaction from cache to prevent memory leaks
            async with self.interaction_cache_lock:
                if interaction_id in self.interaction_cache:
                    del self.interaction_cache[interaction_id]
                    self.logger.debug(f"Removed interaction {interaction_id} from cache after processing.")

    def _create_product_title(self, prompt: str, username: str) -> str:
        """Create a unique, human-readable product title based on the prompt and username."""
        prompt_words = prompt.strip().split()
        first_four_words = ' '.join(prompt_words[:8]).title() if len(prompt_words) >= 8 else ' '.join(prompt_words).title()
        title = f"{first_four_words} Artist Trading Card (ATC) by {username}"
        return title

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

    async def _generate_and_upload_image(self, prompt: str) -> Optional[str]:
        """Generate an image and upload it to Backblaze."""
        try:
            # Removed 'sync_mode' parameter as it's no longer needed
            result = await self.flux_handler.generate_image(prompt)
            if not result or 'images' not in result or not result['images']:
                self.logger.warning("Image generation failed: No images returned.")
                return None

            image_url = result['images'][0]['url']
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

    async def _send_followup(
            self,
            interaction_id: int,
            content: Optional[str] = None,
            embed: Optional[discord.Embed] = None,
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
                    await cached_interaction.interaction.followup.send(
                        content=content,
                        embed=embed,
                        **kwargs
                    )
                    self.logger.info(f"Follow-up sent via original interaction {interaction_id}.")
                    return
                except discord.HTTPException as e:
                    if e.code == 10062:  # Unknown Interaction
                        self.logger.warning(f"Interaction {interaction_id} has expired. Trying other methods.")
                    else:
                        self.logger.error(
                            f"HTTPException when sending follow-up via interaction {interaction_id}: {e}",
                            exc_info=True
                        )
                        return  # If it's a different HTTPException, we should not proceed further
                except discord.DiscordException as e:
                    self.logger.warning(
                        f"Failed to send follow-up via original interaction {interaction_id}: {e}"
                    )

            # Step 2: Attempt to send to the original channel
            if cached_interaction and cached_interaction.channel_id:
                channel = self.bot.get_channel(cached_interaction.channel_id)
                if channel and isinstance(channel, discord.TextChannel):
                    # Ensure we have the bot's member object
                    me = channel.guild.me or await channel.guild.fetch_member(self.bot.user.id)
                    permissions = channel.permissions_for(me)
                    if permissions.send_messages:
                        try:
                            await channel.send(
                                content=content,
                                embed=embed,
                                **kwargs
                            )
                            self.logger.info(
                                f"Follow-up sent to channel {channel.id} for interaction {interaction_id}."
                            )
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
                        try:
                            await user.send(
                                content=content,
                                embed=embed,
                                **kwargs
                            )
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

async def setup(bot: DiscordShopifyBot):
    """Setup function to add the cog to the bot."""
    await bot.add_cog(ImageProductCommand(bot))