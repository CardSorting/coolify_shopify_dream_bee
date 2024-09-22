import os
from typing import Dict, List, Tuple, Union, Optional
from dataclasses import dataclass
import asyncio

import redis.asyncio as redis
from dotenv import load_dotenv

from utils.logger import Logger

load_dotenv()

class CreditSystemError(Exception):
    pass

class RedisConnectionError(CreditSystemError):
    pass

class InsufficientCreditsError(CreditSystemError):
    pass

@dataclass
class RedisConfig:
    host: str
    port: int
    db: int
    password: Optional[str]

class CreditSystem:
    def __init__(self, **kwargs):
        self.config = RedisConfig(
            host=kwargs.get('host', os.getenv('REDIS_HOST', 'localhost')),
            port=int(kwargs.get('port', os.getenv('REDIS_PORT', 6379))),
            db=int(kwargs.get('db', os.getenv('REDIS_DB', 0))),
            password=kwargs.get('password', os.getenv('REDIS_PASSWORD'))
        )
        self.redis_client: Optional[redis.Redis] = None
        self.logger = Logger.get_instance("CreditSystem")

    async def initialize(self):
        """Initialize the Redis client."""
        try:
            self.redis_client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                retry_on_timeout=True,
                health_check_interval=30
            )
            await self.redis_client.ping()
            self.logger.info("Connected to Redis successfully.")
        except redis.RedisError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise RedisConnectionError(f"Unable to connect to Redis: {e}") from e

    async def close(self):
        """Close the Redis client connection."""
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("Redis connection closed.")

    @staticmethod
    def _key(user_id: Union[int, str], key_type: str) -> str:
        """Construct a Redis key for a user and key type."""
        return f"user:{user_id}:{key_type}"

    async def _execute_redis_operation(self, operation, *args):
        """Execute a Redis operation with retry logic."""
        max_retries, retry_delay = 3, 0.1
        for attempt in range(max_retries):
            try:
                return await operation(*args)
            except redis.RedisError as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Redis operation failed after {max_retries} attempts: {e}")
                    raise CreditSystemError(f"Redis operation failed: {e}") from e
                await asyncio.sleep(retry_delay * (2 ** attempt))

    async def add_credit(self, user_id: int, amount: int) -> bool:
        """Add credits to a user's balance."""
        key = self._key(user_id, 'credits')
        try:
            await self._execute_redis_operation(self.redis_client.incrby, key, amount)
            self.logger.debug(f"Added {amount} credits to user {user_id}.")
            return True
        except Exception as e:
            self.logger.error(f"Error adding credits to user {user_id}: {e}", exc_info=True)
            return False

    async def deduct_credit(self, user_id: int, amount: int = 1) -> bool:
        """Deduct credits from a user's balance."""
        key = self._key(user_id, 'credits')
        try:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(key)
                        current = await pipe.get(key)
                        current = int(current) if current else 0
                        if current < amount:
                            self.logger.warning(f"User {user_id} has insufficient credits ({current}).")
                            return False  # Not enough credits
                        pipe.multi()
                        pipe.decrby(key, amount)
                        await pipe.execute()
                        self.logger.debug(f"Deducted {amount} credits from user {user_id}.")
                        return True  # Successful deduction
                    except redis.WatchError:
                        await asyncio.sleep(0.1)
                        continue
        except Exception as e:
            self.logger.error(f"Error in deduct_credit for user {user_id}: {e}", exc_info=True)
            return False  # Error occurred

    async def get_credits(self, user_id: int) -> int:
        """Retrieve the current credit balance for a user."""
        key = self._key(user_id, 'credits')
        try:
            credits = await self._execute_redis_operation(self.redis_client.get, key)
            credits = int(credits) if credits else 0
            self.logger.debug(f"User {user_id} has {credits} credits.")
            return credits
        except Exception as e:
            self.logger.error(f"Error getting credits for user {user_id}: {e}", exc_info=True)
            return 0

    async def can_claim(self, user_id: int) -> Tuple[bool, int]:
        """Check if a user can claim daily credits and return time remaining if not."""
        key = self._key(user_id, 'last_claim')
        try:
            ttl = await self._execute_redis_operation(self.redis_client.ttl, key)
            can_claim = ttl <= 0
            remaining_time = max(0, ttl)
            self.logger.debug(f"User {user_id} can_claim: {can_claim}, ttl: {ttl}")
            return can_claim, remaining_time
        except Exception as e:
            self.logger.error(f"Error in can_claim for user {user_id}: {e}", exc_info=True)
            return False, 0

    async def set_last_claim(self, user_id: int) -> bool:
        """Set the last claim time for a user to prevent multiple claims within 24 hours."""
        key = self._key(user_id, 'last_claim')
        try:
            await self._execute_redis_operation(self.redis_client.set, key, "1", ex=86400)
            self.logger.debug(f"Set last claim time for user {user_id}.")
            return True
        except Exception as e:
            self.logger.error(f"Error setting last claim time for user {user_id}: {e}", exc_info=True)
            return False

    async def reset_credits(self, user_id: int) -> bool:
        """Reset a user's credits to zero."""
        key = self._key(user_id, 'credits')
        try:
            await self._execute_redis_operation(self.redis_client.set, key, 0)
            self.logger.debug(f"Reset credits for user {user_id} to 0.")
            return True
        except Exception as e:
            self.logger.error(f"Error resetting credits for user {user_id}: {e}", exc_info=True)
            return False

    async def set_credits(self, user_id: int, amount: int) -> bool:
        """Set a user's credits to a specific amount."""
        key = self._key(user_id, 'credits')
        try:
            await self._execute_redis_operation(self.redis_client.set, key, amount)
            self.logger.debug(f"Set credits for user {user_id} to {amount}.")
            return True
        except Exception as e:
            self.logger.error(f"Error setting credits for user {user_id}: {e}", exc_info=True)
            return False

    async def get_all_users_with_credits(self) -> List[int]:
        """Retrieve a list of all user IDs who have a credit balance."""
        pattern = self._key("*", 'credits')
        try:
            keys = await self._execute_redis_operation(self.redis_client.keys, pattern)
            user_ids = [int(key.split(":")[1]) for key in keys]
            self.logger.debug(f"Found users with credits: {user_ids}")
            return user_ids
        except Exception as e:
            self.logger.error(f"Error getting all users with credits: {e}", exc_info=True)
            return []

    async def get_credit_leaderboard(self, top_n: int = 10) -> List[Tuple[int, int]]:
        """Get a leaderboard of users with the highest credit balances."""
        try:
            user_ids = await self.get_all_users_with_credits()
            credits_list = await asyncio.gather(*[self.get_credits(user_id) for user_id in user_ids])
            user_credits = list(zip(user_ids, credits_list))
            sorted_leaderboard = sorted(user_credits, key=lambda x: x[1], reverse=True)[:top_n]
            self.logger.debug(f"Credit leaderboard: {sorted_leaderboard}")
            return sorted_leaderboard
        except Exception as e:
            self.logger.error(f"Error getting credit leaderboard: {e}", exc_info=True)
            return []

    async def batch_update_credits(self, updates: Dict[int, int]) -> bool:
        """Batch update credits for multiple users."""
        try:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                for user_id, change in updates.items():
                    key = self._key(user_id, 'credits')
                    pipe.incrby(key, change)
                await pipe.execute()
            self.logger.debug(f"Batch updated credits: {updates}")
            return True
        except Exception as e:
            self.logger.error(f"Error in batch_update_credits: {e}", exc_info=True)
            return False

    async def transfer_credits(self, from_user: int, to_user: int, amount: int) -> bool:
        """Transfer credits from one user to another."""
        from_key = self._key(from_user, 'credits')
        to_key = self._key(to_user, 'credits')
        try:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(from_key)
                        current = await pipe.get(from_key)
                        current = int(current) if current else 0
                        if current < amount:
                            self.logger.warning(f"User {from_user} has insufficient credits ({current}) for transfer.")
                            return False
                        pipe.multi()
                        pipe.decrby(from_key, amount)
                        pipe.incrby(to_key, amount)
                        await pipe.execute()
                        self.logger.info(f"Transferred {amount} credits from user {from_user} to user {to_user}.")
                        return True
                    except redis.WatchError:
                        await asyncio.sleep(0.1)
                        continue
        except Exception as e:
            self.logger.error(f"Error in transfer_credits from user {from_user} to user {to_user}: {e}", exc_info=True)
            return False