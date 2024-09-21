# credit_system.py
import redis.asyncio as redis
import os
from dotenv import load_dotenv
from typing import Tuple

# Load environment variables from .env file
load_dotenv()

class CreditSystem:
    def __init__(self):
        # Initialize Redis client
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            password=os.getenv('REDIS_PASSWORD', None),
            decode_responses=True  # Automatically decode responses to strings
        )

    async def initialize(self):
        """Asynchronously initialize the credit system by pinging Redis."""
        try:
            await self.redis_client.ping()
            print("Connected to Redis successfully.")
        except redis.exceptions.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            raise e

    async def add_credit(self, user_id: int, amount: int) -> None:
        """Add credits to a user's account."""
        key = f"user:{user_id}:credits"
        await self.redis_client.incrby(key, amount)

    async def deduct_credit(self, user_id: int, amount: int) -> bool:
        """
        Deduct credits from a user's account.
        Returns True if successful, False if insufficient credits.
        """
        key = f"user:{user_id}:credits"
        async with self.redis_client.pipeline(transaction=True) as pipe:
            try:
                # Watch the key for changes
                await pipe.watch(key)
                current = await pipe.get(key)
                current = int(current) if current else 0
                if current < amount:
                    await pipe.unwatch()
                    return False
                # Start the transaction
                pipe.multi()
                pipe.decrby(key, amount)
                await pipe.execute()
                return True
            except redis.exceptions.WatchError:
                return False

    async def get_credits(self, user_id: int) -> int:
        """Retrieve the current credit balance of a user."""
        key = f"user:{user_id}:credits"
        current = await self.redis_client.get(key)
        return int(current) if current else 0

    async def can_claim(self, user_id: int) -> Tuple[bool, int]:
        """
        Check if a user can claim credits.
        Returns a tuple (can_claim: bool, remaining_seconds: int).
        """
        key = f"user:{user_id}:last_claim"
        ttl = await self.redis_client.ttl(key)

        if ttl > 0:
            return False, ttl
        else:
            return True, 0

    async def set_last_claim(self, user_id: int) -> None:
        """
        Set the last claim time for a user.
        This sets a key with a 24-hour expiration.
        """
        key = f"user:{user_id}:last_claim"
        await self.redis_client.set(key, "1", ex=86400)  # 24 hours in seconds