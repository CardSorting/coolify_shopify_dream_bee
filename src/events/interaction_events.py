import os
import uuid  # Correct import for uuid
import discord
from discord.ext import commands
from discord import Interaction, ButtonStyle
from handlers.product_handler import ProductHandler
from utils.embed_creator import EmbedCreator

class InteractionEvents(commands.Cog):
    def __init__(self, bot: commands.Bot, product_handler: ProductHandler, embed_creator: EmbedCreator):
        self.bot = bot
        self.product_handler = product_handler
        self.embed_creator = embed_creator
        self._register_events()

    def _register_events(self):
        """Register interaction event handlers for the bot."""
        @self.bot.event
        async def on_interaction(interaction: Interaction):
            if interaction.type == discord.InteractionType.component:
                await self.handle_button_click(interaction)

    async def handle_button_click(self, interaction: Interaction):
        """Handle button interactions such as adding generated images to Shopify."""
        if not interaction.data or 'custom_id' not in interaction.data:
            await interaction.response.send_message("Invalid interaction data.", ephemeral=True)
            return

        custom_id = interaction.data['custom_id']
        if custom_id.startswith('add_to_shopify'):
            await self._handle_add_to_shopify(interaction, custom_id)

    async def _handle_add_to_shopify(self, interaction: Interaction, custom_id: str):
        """Handles the 'Add to Shopify' button click event to add a product."""
        try:
            image_url = custom_id.split('|')[1]
            vendor = interaction.user.name
            product_title = f"Artist Trading Card (ATC) {uuid.uuid4()}"
            product_description = self._get_product_description()
            product_price = 6.99  # Default price for the product

            await self._add_product_to_shopify(interaction, product_title, product_description, image_url, vendor, product_price)
        except Exception as e:
            await interaction.response.send_message(f"Failed to handle the button interaction: {str(e)}", ephemeral=True)

    async def _add_product_to_shopify(self, interaction: Interaction, title: str, description: str, image_url: str, vendor: str, price: float):
        """Adds a product to Shopify and provides feedback to the user."""
        try:
            response = await self.product_handler.add_product_to_shopify(
                title=title,
                description=description,
                image_url=image_url,
                vendor=vendor,
                price=price
            )
            if response:
                embed = self.embed_creator.create_confirmation_embed(
                    title="Product Added Successfully",
                    description=f"Product '{title}' has been added to Shopify with a price of ${price:.2f}."
                )
                await interaction.response.send_message(embed=embed)
            else:
                await interaction.response.send_message("Failed to add product to Shopify. Please try again.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error while adding product to Shopify: {str(e)}", ephemeral=True)

    def _get_product_description(self) -> str:
        """Provides a predefined description for Artist Trading Cards (ATCs)."""
        return """Artist Trading Card (ATC) – 2.5 x 3.5 inches

Discover the charm and creativity of artist trading cards, each meticulously crafted to a precise 2.5 x 3.5 inches. Perfect for art enthusiasts, collectors, and creators alike, these miniature canvases offer endless possibilities for artistic expression.

Key Features:

- **Compact Size:** Measuring 2.5 x 3.5 inches, these cards are the ideal canvas for intricate designs and vibrant artwork.
- **High-Quality Surface:** Made from premium cardstock, each card provides a smooth, durable surface that holds up beautifully to various mediums, including paint, ink, markers, and collage.
- **Versatile Uses:** Whether you're a seasoned artist looking to showcase your work or a collector hunting for unique pieces, these ATCs are perfect for trading, displaying, or gifting.
- **Customization:** Blank and ready for your personal touch, these cards invite you to explore your creativity, make a statement, or capture a moment in a miniaturized format.

Celebrate the art of small-scale creativity with these artist trading cards, where every inch is an opportunity for a masterpiece. Perfect for any art lover looking to expand their collection or add a unique personal touch to their projects."""

def setup(bot: commands.Bot):
    """Sets up the InteractionEvents cog."""
    product_handler = ProductHandler(
        api_key=os.getenv('SHOPIFY_API_KEY'),
        password=os.getenv('SHOPIFY_PASSWORD'),
        shop_name=os.getenv('SHOPIFY_SHOP_NAME')
    )
    embed_creator = EmbedCreator()
    bot.add_cog(InteractionEvents(bot, product_handler, embed_creator))