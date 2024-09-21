import asyncio
from collections import deque
from typing import Any, Callable, Coroutine, Generic, Optional, TypeVar
from contextlib import asynccontextmanager
import logging
import time

T = TypeVar('T')

class QueueEmptyError(Exception):
    """Raised when attempting to dequeue from an empty queue."""

class QueueFullError(Exception):
    """Raised when attempting to enqueue to a full queue."""

class InMemoryQueue(Generic[T]):
    """
    A thread-safe, asynchronous, in-memory queue implementation with advanced features.

    This queue is optimized for high-performance scenarios and includes detailed
    logging, error handling, and metrics tracking.
    """

    def __init__(self, max_size: int = 1000, name: str = "default"):
        self._queue: deque[T] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()
        self._name = name
        self._closed = False
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._last_operation_time = time.time()

        self.logger = logging.getLogger(f"InMemoryQueue.{name}")
        self.logger.setLevel(logging.DEBUG)

    @property
    def name(self) -> str:
        """Return the name of the queue."""
        return self._name

    @property
    def max_size(self) -> int:
        """Return the maximum size of the queue."""
        return self._queue.maxlen

    async def size(self) -> int:
        """Return the current size of the queue."""
        async with self._lock:
            return len(self._queue)

    async def is_empty(self) -> bool:
        """Check if the queue is empty."""
        async with self._lock:
            return len(self._queue) == 0

    async def is_full(self) -> bool:
        """Check if the queue is full."""
        async with self._lock:
            return len(self._queue) == self.max_size

    @asynccontextmanager
    async def _operation_context(self, operation: str):
        """Context manager for queue operations with timing and logging."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.logger.debug(f"{operation} operation completed in {duration:.6f} seconds")
            self._last_operation_time = time.time()

    async def enqueue(self, item: T) -> None:
        """
        Add an item to the queue.

        Raises QueueFullError if the queue is full.
        """
        async with self._operation_context("Enqueue"):
            async with self._lock:
                if self._closed:
                    raise QueueFullError("Cannot enqueue to a closed queue")
                if len(self._queue) >= self.max_size:
                    raise QueueFullError("Queue is full")

                self._queue.append(item)
                self._total_enqueued += 1
                self.logger.info(f"Enqueued item. Queue size: {len(self._queue)}")

    async def dequeue(self) -> T:
        """
        Remove and return an item from the queue.

        Raises QueueEmptyError if the queue is empty.
        """
        async with self._operation_context("Dequeue"):
            async with self._lock:
                if self._closed and len(self._queue) == 0:
                    raise QueueEmptyError("Cannot dequeue from a closed and empty queue")
                if len(self._queue) == 0:
                    raise QueueEmptyError("Queue is empty")

                item = self._queue.popleft()
                self._total_dequeued += 1
                self.logger.info(f"Dequeued item. Queue size: {len(self._queue)}")
                return item

    async def peek(self) -> Optional[T]:
        """Return the next item in the queue without removing it."""
        async with self._lock:
            return self._queue[0] if self._queue else None

    async def clear(self) -> None:
        """Remove all items from the queue."""
        async with self._lock:
            self._queue.clear()
            self.logger.info("Queue cleared")

    async def close(self) -> None:
        """Close the queue, preventing further enqueues."""
        async with self._lock:
            self._closed = True
            self.logger.info("Queue closed")

    async def __aiter__(self):
        """Allow the queue to be used as an async iterator."""
        while True:
            try:
                yield await self.dequeue()
            except QueueEmptyError:
                break

    async def process_queue(self, handler: Callable[[T], Coroutine[Any, Any, None]]) -> None:
        """
        Process items in the queue using the provided handler function.

        This method will continue processing until the queue is closed and empty.
        """
        self.logger.info("Starting queue processing")
        while not self._closed or len(self._queue) > 0:
            try:
                item = await self.dequeue()
                await handler(item)
            except QueueEmptyError:
                await asyncio.sleep(0.1)  # Short sleep to prevent tight loop
            except Exception as e:
                self.logger.error(f"Error processing queue item: {e}", exc_info=True)
        self.logger.info("Queue processing completed")

    async def get_statistics(self) -> dict:
        """Return statistics about the queue's usage."""
        async with self._lock:
            return {
                "name": self.name,
                "current_size": len(self._queue),
                "max_size": self.max_size,
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
                "is_full": len(self._queue) == self.max_size,
                "is_empty": len(self._queue) == 0,
                "is_closed": self._closed,
                "last_operation_time": self._last_operation_time
            }

    @classmethod
    async def from_iterable(cls, iterable, max_size: int = 1000, name: str = "from_iterable"):
        """Create a new queue and populate it from an iterable."""
        queue = cls(max_size, name)
        for item in iterable:
            await queue.enqueue(item)
        return queue