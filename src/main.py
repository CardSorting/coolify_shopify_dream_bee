# src/main.py

import asyncio
import os
from dotenv import load_dotenv
from discord.ext import commands
from discord import Intents
from pathlib import Path

from utils.logger import Logger
from credit_system import CreditSystem
from handlers.backblaze_handler import BackblazeHandler
from handlers.product_handler import ProductHandler
from services.shopify_service import ShopifyService

# Load environment variables
load_dotenv()

class DiscordShopifyBot(commands.Bot):
    def __init__(self, application_id: int):
        intents = Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix='!', intents=intents, application_id=application_id)

        self.logger = Logger.get_instance("DiscordShopifyBot")

        # Initialize services and handlers
        self.credit_system = CreditSystem()
        self.backblaze_handler = BackblazeHandler(
            bucket_name=os.getenv('BACKBLAZE_BUCKET_NAME')
        )
        self.shopify_service = ShopifyService(
            shop_name=os.getenv('SHOPIFY_SHOP_NAME'),
            admin_api_token=os.getenv('SHOPIFY_ADMIN_API_TOKEN')
        )
        self.product_handler = ProductHandler(
            shopify_service=self.shopify_service,
            backblaze_handler=self.backblaze_handler
        )

    async def setup_hook(self):
        try:
            await self.initialize_services()
            await self.load_all_cogs()
            await self.tree.sync()
            self.logger.info('Commands synchronized successfully.')
        except Exception as e:
            self.logger.error(f"Error during setup: {e}", exc_info=True)
            raise

    async def initialize_services(self):
        await self.credit_system.initialize()
        self.logger.info("CreditSystem initialized successfully.")
        await self.shopify_service.initialize()
        self.logger.info("ShopifyService initialized successfully.")
        await self.backblaze_handler.initialize()
        self.logger.info("BackblazeHandler initialized successfully.")

    async def load_all_cogs(self):
        cogs_dir = Path(__file__).resolve().parent / 'cogs'
        for filename in os.listdir(cogs_dir):
            if filename.endswith('.py') and not filename.startswith('_'):
                cog_name = filename[:-3]
                try:
                    await self.load_extension(f'cogs.{cog_name}')
                    self.logger.info(f"Loaded cog: cogs.{cog_name}")
                except Exception as e:
                    self.logger.error(f"Failed to load cog cogs.{cog_name}: {e}", exc_info=True)

    async def on_ready(self):
        self.logger.info(f'Bot connected as {self.user}.')

    async def close(self):
        await self.close_services()
        await super().close()

    async def close_services(self):
        for service in [self.product_handler, self.backblaze_handler, self.credit_system, self.shopify_service]:
            try:
                await service.close()
                self.logger.info(f"{service.__class__.__name__} resources closed.")
            except Exception as e:
                self.logger.error(f"Error closing {service.__class__.__name__}: {e}", exc_info=True)

    async def on_command_error(self, context, exception):
        self.logger.error(f"Error in command {context.command}: {exception}", exc_info=True)
        await context.send("An unexpected error occurred. Please try again later.")

async def main():
    # Validate environment variables
    required_env_vars = [
        'DISCORD_TOKEN', 'SHOPIFY_ADMIN_API_TOKEN', 'SHOPIFY_SHOP_NAME',
        'BACKBLAZE_KEY_ID', 'BACKBLAZE_APPLICATION_KEY', 'BACKBLAZE_BUCKET_NAME',
        'ADMIN_USER_ID', 'APPLICATION_ID', 'FAL_KEY'
    ]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    application_id = int(os.getenv('APPLICATION_ID'))
    bot = DiscordShopifyBot(application_id=application_id)

    try:
        await bot.start(os.getenv('DISCORD_TOKEN'))
    except KeyboardInterrupt:
        bot.logger.info("Bot interrupted by user. Shutting down...")
    except Exception as e:
        bot.logger.error(f"Unexpected error during bot runtime: {e}", exc_info=True)
    finally:
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())