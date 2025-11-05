import time
import threading
import logging

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

        self.lock = threading.Lock()
        
    def acquire(self, weight: int = SpotWeights.KLINES):

        """Method that reserves weights or sleeps until capacity is available"""

        while True:
            with self.lock:
                now = self.clock()
                self.pop_old_locked(now)
            
                # Check if there is enough capacity
                if self.used + weight <= self.max_weight_minute:
                    self.hits.append((now, weight))
                    self.used += weight
                    usage_ratio = self.used /self.max_weight_minute
                    self.capacity_used += usage_ratio
                    self.capacity_sample += 1
                    # logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
                    # logging.info("Throttle usage: %.1f%% (%d/%d)", usage_ratio, self.used, self.max_weight_minute)
                    return 
                
                # Sleep when capacity not available
                sleep_s = self.sleep_until_capacity_unlocked(now)   
            time.sleep(sleep_s + 0.2)
    
    def mean_usage(self):
        
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
        
        return expires_in

    
if __name__ == "__main__":

    print("Testing throttle...")

    throttle = Throttle_(max_weight_minute=5, window_s=3)

    def worker(id: int, weight: int):
        print(f"[Worker {id}] Trying to acquire weight {weight} at {time.time():.2f}")
        throttle.acquire(weight)
        print(f"[Worker {id}] Acquired weight {weight} at {time.time():.2f}")
        time.sleep(1)
        print(f"[Worker {id}] Done at {time.time():.2f}")

    # Start multiple threads
    threads = []
    weights = [2, 2, 2]
    for i, w in enumerate(weights):
        t = threading.Thread(target=worker, args=(i+1, w))
        t.start()
        threads.append(t)
        time.sleep(0.1)

    for t in threads:
        t.join()

    print("Throttle test completed.")