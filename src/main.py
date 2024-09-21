import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
import aiohttp
import discord
from discord.ext import commands
from discord import Intents

from utils.logger import Logger
from credit_system import CreditSystem

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
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID'))

# Validate environment variables
required_env_vars = [
    'DISCORD_TOKEN', 'SHOPIFY_ADMIN_API_TOKEN', 'SHOPIFY_SHOP_NAME',
    'BACKBLAZE_KEY_ID', 'BACKBLAZE_APPLICATION_KEY', 'BACKBLAZE_BUCKET_NAME',
    'REDIS_HOST', 'REDIS_PORT', 'REDIS_DB', 'ADMIN_USER_ID'
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Set up intents
intents = Intents.default()
intents.message_content = True
intents.members = True

class DiscordShopifyBot(commands.Bot):
    def __init__(self, credit_system: CreditSystem):
        super().__init__(command_prefix='!', intents=intents)
        self.session = aiohttp.ClientSession()
        self.logger = Logger.get_instance("DiscordShopifyBot")
        self.credit_system = credit_system

        # Store credentials as bot attributes
        self.SHOPIFY_ADMIN_API_TOKEN = SHOPIFY_ADMIN_API_TOKEN
        self.SHOPIFY_SHOP_NAME = SHOPIFY_SHOP_NAME
        self.BACKBLAZE_KEY_ID = BACKBLAZE_KEY_ID
        self.BACKBLAZE_APPLICATION_KEY = BACKBLAZE_APPLICATION_KEY
        self.BACKBLAZE_BUCKET_NAME = BACKBLAZE_BUCKET_NAME

    async def setup_hook(self):
        """Setup tasks to run before the bot is fully ready."""
        try:
            await self.credit_system.initialize()
            self.logger.info("CreditSystem initialized successfully.")
            await self.load_all_cogs()
            await self.sync_commands()
        except Exception as e:
            self.logger.error(f"Error during setup: {e}", exc_info=True)
            raise

    async def load_all_cogs(self):
        """Load all cogs from the 'cogs' directory."""
        cogs_dir = Path(__file__).resolve().parent / 'cogs'
        for filename in os.listdir(cogs_dir):
            if filename.endswith('.py'):
                cog = filename[:-3]
                try:
                    await self.load_extension(f'cogs.{cog}')
                    self.logger.info(f"Loaded cog: cogs.{cog}")
                except commands.ExtensionAlreadyLoaded:
                    self.logger.warning(f"Cog already loaded: cogs.{cog}")
                except commands.ExtensionFailed as e:
                    self.logger.error(f"Failed to load cog cogs.{cog}: {e}", exc_info=True)
                except Exception as e:
                    self.logger.error(f"Unexpected error loading cog cogs.{cog}: {e}", exc_info=True)

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
            # Unload all cogs gracefully
            for cog in list(self.cogs):
                try:
                    await self.unload_extension(f'cogs.{cog.lower()}')
                    self.logger.info(f"Unloaded cog: cogs.{cog.lower()}")
                except Exception as e:
                    self.logger.error(f"Failed to unload cog cogs.{cog.lower()}: {e}", exc_info=True)

            # Close Redis connection (use aclose() instead of close())
            try:
                await self.credit_system.redis_client.aclose()
                self.logger.info("Redis connection closed.")
            except Exception as e:
                self.logger.error(f"Error closing Redis connection: {e}")

            # Close aiohttp session
            await self.session.close()
            self.logger.info("Aiohttp ClientSession closed.")

            # Proceed to close the bot
            await super().close()


async def main():
    """Main function to initialize and run the bot."""
    credit_system = CreditSystem()
    bot = DiscordShopifyBot(credit_system)

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