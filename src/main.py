# src/main.py

import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
import aiohttp
from discord.ext import commands
from discord import Intents

from utils.logger import Logger
from credit_system import CreditSystem
from handlers.backblaze_handler import BackblazeHandler
from handlers.product_handler import ProductHandler
from services.shopify_service import ShopifyService

# Load environment variables
load_dotenv()

# Environment variables
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
SHOPIFY_ADMIN_API_TOKEN = os.getenv('SHOPIFY_ADMIN_API_TOKEN')
SHOPIFY_SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME')
BACKBLAZE_KEY_ID = os.getenv('BACKBLAZE_KEY_ID')
BACKBLAZE_APPLICATION_KEY = os.getenv('BACKBLAZE_APPLICATION_KEY')
BACKBLAZE_BUCKET_NAME = os.getenv('BACKBLAZE_BUCKET_NAME')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
ADMIN_USER_ID = os.getenv('ADMIN_USER_ID')
APPLICATION_ID = int(os.getenv('APPLICATION_ID'))
FAL_KEY = os.getenv('FAL_KEY')  # Ensure FAL_KEY is loaded

# Validate environment variables
required_env_vars = [
    'DISCORD_TOKEN', 'SHOPIFY_ADMIN_API_TOKEN', 'SHOPIFY_SHOP_NAME',
    'BACKBLAZE_KEY_ID', 'BACKBLAZE_APPLICATION_KEY', 'BACKBLAZE_BUCKET_NAME',
    'ADMIN_USER_ID', 'APPLICATION_ID', 'FAL_KEY'
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Set up intents
intents = Intents.default()
intents.message_content = True
intents.members = True

class DiscordShopifyBot(commands.Bot):
    def __init__(
        self,
        credit_system: CreditSystem,
        backblaze_handler: BackblazeHandler,
        shopify_service: ShopifyService,
        product_handler: ProductHandler,
        application_id: int,
    ):
        super().__init__(command_prefix='!', intents=intents, application_id=application_id)
        self.logger = Logger.get_instance("DiscordShopifyBot")

        # Initialize session
        self.session = aiohttp.ClientSession()

        # Assign the services and handlers
        self.credit_system = credit_system
        self.backblaze_handler = backblaze_handler
        self.shopify_service = shopify_service
        self.product_handler = product_handler

    async def setup_hook(self):
        """Setup tasks to run before the bot is fully ready."""
        try:
            # Initialize services
            await self.initialize_services()

            # Load all cogs
            await self.load_all_cogs()

            # Synchronize commands
            await self.tree.sync()
            self.logger.info('Commands synchronized successfully.')

        except Exception as e:
            self.logger.error(f"Error during setup: {e}", exc_info=True)
            raise

    async def initialize_services(self):
        """Initialize all services and handlers."""
        await self.credit_system.initialize()
        self.logger.info("CreditSystem initialized successfully.")

        await self.shopify_service.initialize()
        self.logger.info("ShopifyService initialized successfully.")

        await self.backblaze_handler.initialize()
        self.logger.info("BackblazeHandler initialized successfully.")

        # Initialize any other services if needed

    async def load_all_cogs(self):
        """Load all cogs from the 'cogs' directory."""
        cogs_dir = Path(__file__).resolve().parent / 'cogs'
        for filename in os.listdir(cogs_dir):
            if filename.endswith('.py') and not filename.startswith('_'):
                cog_name = filename[:-3]
                try:
                    await self.load_extension(f'cogs.{cog_name}')
                    self.logger.info(f"Loaded cog: cogs.{cog_name}")
                except commands.ExtensionAlreadyLoaded:
                    self.logger.warning(f"Cog already loaded: cogs.{cog_name}")
                except commands.NoEntryPointError:
                    self.logger.error(f"No setup function found in cog: cogs.{cog_name}")
                except commands.ExtensionFailed as e:
                    self.logger.error(f"Failed to load cog cogs.{cog_name}: {e}", exc_info=True)
                except Exception as e:
                    self.logger.error(f"Unexpected error loading cog cogs.{cog_name}: {e}", exc_info=True)

    async def on_ready(self):
        """Event handler for when the bot is ready."""
        self.logger.info(f'Bot connected as {self.user}.')

    async def close(self):
        """Override the close method to perform additional cleanup."""
        # Unload all cogs gracefully
        for extension in list(self.extensions):
            try:
                await self.unload_extension(extension)
                self.logger.info(f"Unloaded extension: {extension}")
            except Exception as e:
                self.logger.error(f"Failed to unload extension {extension}: {e}", exc_info=True)

        # Close services and handlers
        await self.close_services()

        # Close aiohttp session
        await self.session.close()
        self.logger.info("Aiohttp ClientSession closed.")

        # Proceed to close the bot
        await super().close()

    async def close_services(self):
        """Close all services and handlers."""
        try:
            await self.product_handler.close()
            self.logger.info("ProductHandler resources closed.")
        except Exception as e:
            self.logger.error(f"Error closing ProductHandler: {e}", exc_info=True)

        try:
            await self.backblaze_handler.close()
            self.logger.info("BackblazeHandler resources closed.")
        except Exception as e:
            self.logger.error(f"Error closing BackblazeHandler: {e}", exc_info=True)

        try:
            await self.credit_system.close()
            self.logger.info("CreditSystem resources closed.")
        except Exception as e:
            self.logger.error(f"Error closing CreditSystem: {e}", exc_info=True)

        try:
            await self.shopify_service.close()
            self.logger.info("ShopifyService resources closed.")
        except Exception as e:
            self.logger.error(f"Error closing ShopifyService: {e}", exc_info=True)

    async def on_command_error(self, context, exception):
        """Global error handler for commands."""
        self.logger.error(f"Error in command {context.command}: {exception}", exc_info=True)
        await context.send("An unexpected error occurred. Please try again later.")

async def main():
    """Main function to initialize and run the bot."""
    # Initialize CreditSystem
    credit_system = CreditSystem(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD
    )

    # Initialize BackblazeHandler
    backblaze_handler = BackblazeHandler(
        key_id=BACKBLAZE_KEY_ID,
        application_key=BACKBLAZE_APPLICATION_KEY,
        bucket_name=BACKBLAZE_BUCKET_NAME
    )

    # Initialize ShopifyService
    shopify_service = ShopifyService(
        shop_name=SHOPIFY_SHOP_NAME,
        admin_api_token=SHOPIFY_ADMIN_API_TOKEN
    )

    # Initialize ProductHandler
    product_handler = ProductHandler(
        shopify_service=shopify_service,
        backblaze_handler=backblaze_handler
    )

    # Initialize the bot with all services and handlers
    bot = DiscordShopifyBot(
        credit_system=credit_system,
        backblaze_handler=backblaze_handler,
        shopify_service=shopify_service,
        product_handler=product_handler,
        application_id=APPLICATION_ID
    )

    try:
        # Start the bot
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        bot.logger.info("Bot interrupted by user. Shutting down...")
    except Exception as e:
        bot.logger.error(f"Unexpected error during bot runtime: {e}", exc_info=True)
    finally:
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())