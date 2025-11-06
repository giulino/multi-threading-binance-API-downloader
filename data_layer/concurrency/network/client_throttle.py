import asyncio
import time
from collections import deque
from typing import Deque, Tuple

class SpotWeights:  
      """Endpoint weights for Binance Spot REST."""
      KLINES = 2

class PerpFuturesWeights:  
      """Endpoint weights for Binance Perpetual Futures REST."""
      KLINES = 2

class Throttle_:

    """
    Throttle to control weights usage for
    Binance Spot REST API    
    """

    def __init__(
            self,
            max_weight_minute: int = 6000,
            window_s: int = 60,
            clock = time.monotonic,
            min_sleep: float = 2.0
    ):
        
        self.max_weight_minute = max_weight_minute
        self.window_s = window_s
        self.clock = clock
        self.min_sleep = min_sleep
        
        self.hits: Deque[Tuple[float, int]] = deque()
        self.used = 0
        self.capacity_used = 0
        self.capacity_sample = 0
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int = SpotWeights.KLINES):

        """Method that reserves weights or sleeps until capacity is available"""

        while True:
            async with self._lock:
                now = self.clock()
                self.pop_old_locked(now)
            
                # Check if there is enough capacity
                if self.used + weight <= self.max_weight_minute:
                    self.hits.append((now, weight))
                    self.used += weight
                    usage_ratio = self.used / self.max_weight_minute
                    self.capacity_used += usage_ratio
                    self.capacity_sample += 1
                    return 
                
                # Sleep when capacity not available
                sleep_s = self.sleep_until_capacity_unlocked(now)   
            await asyncio.sleep(sleep_s + 0.2)
    
    def mean_usage(self):
        
        if self.capacity_sample == 0:
            return 0.0
        average_usage = self.capacity_used / self.capacity_sample

        return average_usage   

    def pop_old_locked(self, now: float):
        
        """Method that drop entries older than window_s and adjust running sum."""
        
        cutoff = now - self.window_s
        
        # Check if the oldest element is expired
        while self.hits and self.hits[0][0] < cutoff:  
            _, w = self.hits.popleft()
            self.used -= w
            if self.used < 0:
                self.used = 0

    def sleep_until_capacity_unlocked(self, now: float):
        
        """
        Next capacity change happens when the oldest reservation expires.
        Return a small positive sleep duration until that time.
        """
        
        # Check if there are active reservations
        if not self.hits:
            return self.min_sleep
        
        oldest_ts, _ = self.hits[0]
        expires_in = (oldest_ts + self.window_s) - now 
        
        return max(expires_in, 0.0)

    
if __name__ == "__main__":

    async def _demo():
        print("Testing async throttle...")

        throttle = Throttle_(max_weight_minute=5, window_s=3)

        async def worker(worker_id: int, weight: int):
            print(f"[Worker {worker_id}] Waiting to acquire weight {weight} at {time.time():.2f}")
            await throttle.acquire(weight)
            print(f"[Worker {worker_id}] Acquired weight {weight} at {time.time():.2f}")
            await asyncio.sleep(1.0)
            print(f"[Worker {worker_id}] Finished at {time.time():.2f}")

        await asyncio.gather(
            *(worker(idx + 1, weight) for idx, weight in enumerate([2, 2, 2]))
        )

        print("Throttle test completed.")

    asyncio.run(_demo())
