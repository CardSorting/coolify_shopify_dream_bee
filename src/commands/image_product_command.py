import os
import uuid
import json
import discord
from discord import app_commands, Interaction
from discord.ext import commands
from handlers.flux_image_handler import FluxImageHandler
from handlers.backblaze_handler import BackblazeHandler
from utils.embed_creator import EmbedCreator
from utils.logger import Logger

# In-memory cache for image URLs
image_url_cache = {}

class ImageProductCommand(app_commands.Group):
    def __init__(self, bot: commands.Bot, flux_handler: FluxImageHandler, backblaze_handler: BackblazeHandler, embed_creator: EmbedCreator):
        super().__init__(name="product", description="Commands for product generation")
        self.bot = bot
        self.flux_handler = flux_handler
        self.backblaze_handler = backblaze_handler
        self.embed_creator = embed_creator
        self.logger = Logger.get_instance("ImageProductCommand")

    @app_commands.command(name='generate', description="Generate a product image using AI")
    @app_commands.describe(prompt="The prompt to generate the image with")
    async def generate_product(self, interaction: Interaction, prompt: str):
        await interaction.response.defer()
        try:
            # Step 1: Generate Image Using FLUX.1 Model
            image_url = await self.flux_handler.generate_image(prompt)
            if not image_url:
                await interaction.followup.send(
                    "Failed to generate image. The FLUX.1 API might be experiencing issues. Please try again later or contact support if the problem persists."
                )
                return

            # Step 2: Download Image Content
            image_content = await self.flux_handler.download_image(image_url)
            if not image_content:
                await interaction.followup.send(
                    "Failed to download the generated image. Please try again.",
                    ephemeral=True
                )
                return

            # Step 3: Upload Image to Backblaze
            file_name = f"ATC_{uuid.uuid4()}.jpg"
            backblaze_url = await self.backblaze_handler.upload_image(file_name, image_content)
            if not backblaze_url:
                await interaction.followup.send(
                    "Successfully generated the image, but failed to upload it. Please try again or contact support if the problem persists."
                )
                return

            # Step 4: Create Embed and Present Options
            embed = self.embed_creator.create_image_embed(backblaze_url, prompt)
            view = await self._create_product_options_view(backblaze_url)  # Pass Backblaze URL
            await interaction.followup.send(embed=embed, view=view)
        except Exception as e:
            self.logger.error(f"Error in generate_product: {e}")
            await interaction.followup.send(
                f"An unexpected error occurred: {str(e)}. Please try again or contact support if the problem persists."
            )

    async def _create_product_options_view(self, image_url: str) -> discord.ui.View:
        view = discord.ui.View(timeout=None)
        short_id = str(uuid.uuid4())[:8]
        image_url_cache[short_id] = image_url  # Store in module-level cache
        button = discord.ui.Button(
            label="Add to Shop",
            style=discord.ButtonStyle.green,
            custom_id=f"add_to_shopify|{short_id}"
        )
        view.add_item(button)
        return view

    @app_commands.command(name='debug_cache', description="Debug command to view the image URL cache")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_cache(self, interaction: Interaction):
        cache_content = json.dumps(image_url_cache, indent=2)
        await interaction.response.send_message(f"Image URL Cache:\n```json\n{cache_content}\n```", ephemeral=True)

async def setup(bot: commands.Bot):
    flux_handler = FluxImageHandler()
    backblaze_handler = BackblazeHandler(
        key_id=os.getenv('BACKBLAZE_KEY_ID'),
        application_key=os.getenv('BACKBLAZE_APPLICATION_KEY'),
        bucket_name=os.getenv('BACKBLAZE_BUCKET_NAME')
    )
    embed_creator = EmbedCreator()
    image_product_cmd = ImageProductCommand(bot, flux_handler, backblaze_handler, embed_creator)
    bot.tree.add_command(image_product_cmd)