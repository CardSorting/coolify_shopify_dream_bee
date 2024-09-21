import aiohttp
import os
import asyncio
from dotenv import load_dotenv
import discord
from discord.ext import commands
from discord import Intents
from utils.logger import Logger

# Load environment variables from .env file
load_dotenv()

# Environment variables for Discord and Shopify credentials
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
SHOPIFY_ADMIN_API_TOKEN = os.getenv('SHOPIFY_ADMIN_API_TOKEN')
SHOPIFY_SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME')
BACKBLAZE_KEY_ID = os.getenv('BACKBLAZE_KEY_ID')
BACKBLAZE_APPLICATION_KEY = os.getenv('BACKBLAZE_APPLICATION_KEY')
BACKBLAZE_BUCKET_NAME = os.getenv('BACKBLAZE_BUCKET_NAME')

# Validate environment variables
required_env_vars = [
    'DISCORD_TOKEN',
    'SHOPIFY_ADMIN_API_TOKEN',
    'SHOPIFY_SHOP_NAME',
    'BACKBLAZE_KEY_ID',
    'BACKBLAZE_APPLICATION_KEY',
    'BACKBLAZE_BUCKET_NAME'
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    missing = ', '.join(missing_vars)
    raise EnvironmentError(f"Missing required environment variables: {missing}")

# Discord intents setup
intents = Intents.default()
intents.message_content = True
intents.members = True  # Required for fetching user information

class DiscordShopifyBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)

        # Initialize aiohttp session
        self.session = aiohttp.ClientSession()
        # Initialize logger
        self.logger = Logger.get_instance("DiscordShopifyBot")

        # Store credentials as bot attributes for access within cogs
        self.SHOPIFY_ADMIN_API_TOKEN = SHOPIFY_ADMIN_API_TOKEN
        self.SHOPIFY_SHOP_NAME = SHOPIFY_SHOP_NAME
        self.BACKBLAZE_KEY_ID = BACKBLAZE_KEY_ID
        self.BACKBLAZE_APPLICATION_KEY = BACKBLAZE_APPLICATION_KEY
        self.BACKBLAZE_BUCKET_NAME = BACKBLAZE_BUCKET_NAME

    async def setup_hook(self):
        """Setup tasks to run before the bot is fully ready."""
        # Load the ImageProductCommand cog
        try:
            await self.load_extension('commands.image_product_command')
            self.logger.info("ImageProductCommand cog loaded successfully.")
        except commands.ExtensionAlreadyLoaded:
            self.logger.warning("ImageProductCommand cog is already loaded.")
        except commands.ExtensionFailed as e:
            self.logger.error(f"Failed to load ImageProductCommand cog: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Unexpected error loading ImageProductCommand cog: {e}", exc_info=True)

        # Sync commands globally (ensure slash commands are registered)
        await self.sync_commands()

    async def sync_commands(self):
        """Synchronize the bot's application commands with Discord."""
        try:
            await self.tree.sync()
            self.logger.info('Synced commands globally.')
        except discord.errors.Forbidden as e:
            self.logger.error(f'Failed to sync commands due to missing access: {e}')
        except Exception as e:
            self.logger.error(f'Unexpected error during command synchronization: {e}', exc_info=True)

    async def on_ready(self):
        """Event handler for when the bot is ready."""
        self.logger.info(f'Bot connected as {self.user} and commands are synced globally.')

    async def close(self):
        """Override the close method to perform additional cleanup."""
        # Attempt to unload the ImageProductCommand cog gracefully
        image_product_cog = self.get_cog('ImageProductCommand')
        if image_product_cog and hasattr(image_product_cog, 'cog_unload'):
            image_product_cog.cog_unload()  # Handles closing queues and tasks
            self.logger.info("ImageProductCommand cog unloaded successfully.")
        else:
            self.logger.warning("ImageProductCommand cog not found or lacks a cog_unload method.")
        async def close(self):
            # Close aiohttp session
            await self.session.close()
            self.logger.info("Aiohttp ClientSession closed.")
        
        # Proceed to close the bot
        await super().close()

async def main():
    """Main function to initialize and run the bot."""
    bot = DiscordShopifyBot()
    try:
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        bot.logger.info("Bot interrupted by user. Shutting down...")
    except Exception as e:
        bot.logger.error(f"Unexpected error during bot runtime: {e}", exc_info=True)
    finally:
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())