from concurrency.orchestration.spot_throttle import spot_throttle, SpotWeights

from math import floor

class Autoscaler:
    def __init__(
            self,
            throttle: spot_throttle, # stateful object tracking relevant information
            min_workers: int = 1,
            max_workers: int = 100,
    ):
        
        self.throttle = throttle
        self.min_workers = min_workers
        self.max_workers = max_workers

    def workers(self, rtt_s: float):

        # Define the total request per minute
        request_minute = self.throttle.max_weight_minute / SpotWeights.KLINES

        # Calculate total workers 
        tot_workers = floor(request_minute / (60 / rtt_s))
    
        return min(max(tot_workers, self.min_workers), self.max_workers)


if __name__ == "__main__":
    import time

    print("Testing Autoscaler...")

    # Create a fake spot_throttle with a low max weight for testing
    throttle = spot_throttle(max_weight_minute=10, window_s=60)

    # Create Autoscaler
    autoscaler = Autoscaler(throttle=throttle, min_workers=1, max_workers=50)

    # Test with different RTTs
    test_rtts = [0.05, 0.2, 0.5, 1.0, 2.0]  # seconds

    for rtt in test_rtts:
        workers = autoscaler.workers(rtt_s=rtt)
        print(f"RTT={rtt}s -> suggested workers: {workers}")

    print("Autoscaler test completed.")
