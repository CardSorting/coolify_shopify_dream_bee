import os
import asyncio
from dotenv import load_dotenv

import discord
from discord.ext import commands
from discord import Intents, Interaction, InteractionType

from handlers.flux_image_handler import FluxImageHandler
from handlers.backblaze_handler import BackblazeHandler
from handlers.product_handler import ProductHandler
from utils.embed_creator import EmbedCreator
from utils.logger import Logger
from commands import image_product_command

# Load environment variables from .env file
load_dotenv()

# Environment variables for Discord and Shopify credentials
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
SHOPIFY_ADMIN_API_TOKEN = os.getenv('SHOPIFY_ADMIN_API_TOKEN')
SHOPIFY_SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME')
BACKBLAZE_KEY_ID = os.getenv('BACKBLAZE_KEY_ID')
BACKBLAZE_APPLICATION_KEY = os.getenv('BACKBLAZE_APPLICATION_KEY')
BACKBLAZE_BUCKET_NAME = os.getenv('BACKBLAZE_BUCKET_NAME')

# Discord intents setup
intents = Intents.default()
intents.message_content = True

class DiscordShopifyBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)

        # Initialize utilities and handlers
        self.logger = Logger.get_instance("DiscordShopifyBot")
        self.embed_creator = EmbedCreator()
        self.flux_handler = FluxImageHandler()
        self.backblaze_handler = BackblazeHandler(
            key_id=BACKBLAZE_KEY_ID,
            application_key=BACKBLAZE_APPLICATION_KEY,
            bucket_name=BACKBLAZE_BUCKET_NAME
        )
        self.product_handler = ProductHandler(
            admin_api_token=SHOPIFY_ADMIN_API_TOKEN,
            shop_name=SHOPIFY_SHOP_NAME,
            backblaze_handler=self.backblaze_handler
        )

    async def setup_hook(self):
        await image_product_command.setup(self)
        await self.sync_commands()

    async def sync_commands(self):
        try:
            await self.tree.sync()
            self.logger.info('Synced commands globally.')
        except discord.errors.Forbidden as e:
            self.logger.error(f'Failed to sync commands due to missing access: {e}')

    async def on_ready(self):
        self.logger.info(f'Bot connected as {self.user} and commands are synced globally.')

    async def on_interaction(self, interaction: Interaction):
        if interaction.type == InteractionType.component:
            await self.handle_button_click(interaction)

    async def handle_button_click(self, interaction: Interaction):
        """
        Handles button click interactions, specifically the 'add to Shopify' action.
        """
        custom_id = interaction.data.get('custom_id')
        self.logger.debug(f"Received interaction with custom_id: {custom_id}")
        if not custom_id:
            self.logger.error("Invalid interaction data. No custom_id found.")
            await interaction.response.send_message("Invalid interaction data.", ephemeral=True)
            return

        if custom_id.startswith('add_to_shopify'):
            await self._handle_add_to_shopify(interaction, custom_id)

    async def _handle_add_to_shopify(self, interaction: Interaction, custom_id: str):
        """
        Handles the 'Add to Shopify' button click event by retrieving the image URL from cache
        and creating a product in Shopify with that image.
        """
        try:
            short_id = custom_id.split('|')[1]
            self.logger.debug(f"Handling 'add to Shopify' with short_id: {short_id}")

            image_url = image_product_command.image_url_cache.get(short_id)
            if not image_url:
                self.logger.error(f"Image URL not found in cache for short_id: {short_id}")
                await interaction.response.send_message(
                    "Failed to retrieve image URL. Please try generating the image again.",
                    ephemeral=True
                )
                return

            # Log the retrieved image URL
            self.logger.debug(f"Retrieved image URL from cache: {image_url}")

            # Prepare product data with the retrieved image URL
            product_data = self._create_product_data(interaction, image_url)
            self.logger.debug(f"Created product data: {product_data}")

            await self._add_product_to_shopify(interaction, product_data)
        except Exception as e:
            self.logger.error(f"Failed to handle the button interaction: {e}", exc_info=True)
            await interaction.response.send_message(
                "An error occurred while processing your request. Please try again later.",
                ephemeral=True
            )

    def _create_product_data(self, interaction: Interaction, image_url: str) -> dict:
        """
        Creates product data for Shopify based on the provided image URL and interaction details.
        """
        product_data = {
            "title": f"Artist Trading Card (ATC) {interaction.id}",
            "description": self._get_product_description(),
            "image_url": image_url,  # Pass the correct image URL
            "vendor": interaction.user.name,
            "price": 6.99
        }
        self.logger.debug(f"Product data created for Shopify: {product_data}")
        return product_data

    async def _add_product_to_shopify(self, interaction: Interaction, product_data: dict):
        """
        Adds a product to Shopify using the product data and sends feedback to the user.
        """
        try:
            # Log before adding the product
            self.logger.debug(f"Attempting to add product to Shopify: {product_data}")

            response = await self.product_handler.add_product_to_shopify(product_data)

            # Log the response received from Shopify
            self.logger.debug(f"Shopify response: {response}")

            if response and 'product_url' in response:
                embed = self.embed_creator.create_confirmation_embed(
                    title="Product Added Successfully",
                    description=(
                        f"Product '{product_data['title']}' has been added to Shopify "
                        f"with a price of ${product_data['price']:.2f}.\n\n"
                        f"View product: {response['product_url']}"
                    )
                )
                await interaction.response.send_message(embed=embed)
            else:
                self.logger.error("Failed to add product to Shopify or product URL is missing in the response.")
                await interaction.response.send_message(
                    "Failed to add product to Shopify. Please try again.",
                    ephemeral=True
                )
        except Exception as e:
            self.logger.error(f"Error while adding product to Shopify: {e}", exc_info=True)
            await interaction.response.send_message(
                "An error occurred while adding the product. Please try again later.",
                ephemeral=True
            )

    def _get_product_description(self) -> str:
        """
        Provides a static description for the product.
        """
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

    async def close(self):
        await super().close()

async def main():
    bot = DiscordShopifyBot()
    try:
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        bot.logger.info("Bot interrupted. Shutting down...")
    finally:
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())