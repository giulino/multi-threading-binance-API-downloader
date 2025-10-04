import time
import threading

from math import floor
from collections import deque
from typing import Deque, Dict, Optional, Tuple

# weight/request
SPOT_WEIGHT = 2

class SpotThrottle:

    """
    Throttle to control weights usage for
    Binance Spot REST API    
    """

    def __init__(
            self,
            max_weight_minute: 5990,
            min_poll_sleep_s: 0.25,
            window_s: 60, # binance 60s rolling time window
            clock: time.monotonic
    ):
        
        self.max_weight_minute = max_weight_minute
        self.poll_sleep = min_poll_sleep_s
        self.window_s = window_s
        self.clock = clock
        
        # Internal sliding window of timestamps/weights
        self.hits: Deque[Tuple[float, int]] = deque()
        self.used: 0
        self.lock = threading.Lock()

    def acquire(self, weight : 2):

        while True:
            with self.lock:
                now = self.clock
                self.pop_old_locked(now)
            
                # Check if there is enough capacity
                if self.used + weight <= self.limit:
                    self.hits.append((now, weight))
                    self.used += weight
                    return
                
                # Sleep when capacity not available
                sleep_s = self.sleep_until_capacity_unlocked(now)
            # The program suspends the thread   
            time.sleep(sleep_s + 0.2)

    def max_workers(self, min_workers: int = 1, 
                    max_workers: int = 50,
                    rtt_s: float = 0.5
                    ):
        
        # Define the total request per minute
        request_minute = self.max_weight_minute / SPOT_WEIGHT
        
        # Calculate total workers 
        tot_workers = floor((request_minute * rtt_s) / 60)

        return min(max(tot_workers, min_workers), max_workers)

    def pop_old_locked(self, now: float):
        """Drop entries older than window_s; adjust running sum."""
        
        cutoff = now - self.window_s
        
        # Check if the oldest element is expired
        while self.hits and self.hits[0][0] < cutoff: # deque is non-empty and the first element (timestamp) of the oldest deque (first zero)
            # Removes the holdest element of the deque  
            _, w = self.hits.popleft() # tuple unpacking, disregard the timestamp and pop the weight
            # Reduce the running total
            self.used -= w

    def sleep_until_capacity_unlocked(self, now: float):
        """
        Next capacity change happens when the oldest reservation expires.
        Return a small positive sleep duration until that time.
        """
        # Check if there are active reservations
        if not self.hits:
            return self.min_sleep
        # Check the timestamp of the oldest deque
        oldest_ts, _ = self.hits[0]
        # Calculate when the oldest deque will espire
        expires_in = (oldest_ts + self.window_s) - now 
        return expires_in

