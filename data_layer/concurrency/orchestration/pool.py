from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
import numpy as np

from typing import Callable, Any, Tuple, Dict, List

# import project dependencies
from concurrency.network.client_http import HttpClient, HttpResult
from concurrency.orchestration.spot_throttle import spot_throttle, SpotWeights
from concurrency.orchestration.autoscaler import Autoscaler

class WorkerPool:
    """
    Concurrent job runner for Binance Spot downloads with built-in rate-limit
    coordination and adaptive concurrency.

    Overview
    --------
    WorkerPool consumes "jobs" (dicts describing one REST request window) from
    an internal Queue and executes them in parallel on a fixed set of threads.
    Concurrency is governed by a semaphore so we can raise/lower the number of
    simultaneous in-flight requests without recreating threads. Before each
    request, a shared spot_throttle is consulted to pre-reserve REQUEST_WEIGHT,
    guaranteeing we never overshoot Binance’s sliding 60s limits. After each
    request, we update an rtt_s EMA and autoscale concurrency every n completetions.

    Key responsibilities
    --------------------
    - Job dispatch: pull jobs from a Queue and run them on a thread pool.
    - Concurrency control: semaphore reflects current permitted parallelism.
    - Rate limiting: call throttle.acquire(weight) *before* every request.
    - rtt_s feedback: feed res.elapsed_ms to a shared EMA estimator.
    - Autoscaling: periodically ask an autoscaler policy for a new target and
      adjust permits smoothly (scale up via semaphore release; scale down by
      absorbing permits on completion).
    - Error handling: back off/requeue on RateLimitError; requeue on generic
      exceptions; always mark Queue tasks done.
    """
    def __init__(
            self, 
            http: HttpClient,  # shared HTTP client (handles retries, errors, etc.)
            throttle: spot_throttle,  # shared spot_throttle instance for rate-limiting
            request_builder: Callable[[Dict[str, Any]], Tuple[str, Dict[str, Any]]],  # builds endpoint + params
            response: Callable[[HttpResult, Dict[str, Any]], None],  # handles response
            weight_per_request: int = SpotWeights.KLINES,  # Binance weight per request (default 2)
            max_threads: int = 100,  # maximum thread count
            initial_concurrency: int = 1,  # initial active workers
            market_type: str = "spot"
        ):

        # ---- Assign injected dependencies ----
        self.http = http
        self.throttle = throttle
        self.request_builder = request_builder
        self.response = response
        self.weight = int(weight_per_request)


        # ---- Internal infrastructure 
        self._execution = ThreadPoolExecutor(max_workers=int(max_threads), thread_name_prefix="wrkr")  # thread pool
        self._semaphore = threading.BoundedSemaphore(max(1, int(initial_concurrency)))  # controls concurrency dynamically

        # ---- Concurrency accounting ----
        self._lock = threading.Lock()  # protects shared counters below
    
        # ---- Runtime state ----
        self._max_threads = int(max_threads)

        # type of data
        self._market_type = market_type
    
    # ======================
    # Worker Execution Loop
    # ======================

    def _run_job(self, job: Dict[str, Any], max_retries: int = 3):
        """Run a single job for a fixed number of workers"""
        
        retries = 0
        while True:
            try:    
                # Acquire concurrency permit
                self._semaphore.acquire()
                
                # Pre-reserve weight in throttle
                self.throttle.acquire(self.weight)

                # Build request
                path, params = self.request_builder(job, self._market_type)

                # Send request
                try: 
                    result = self.http.get(path, params=params)
                except Exception as e:
                    # print(f"[_run_job DEBUG] Execution stopped - Job {job} failed with exception: {e}")
                    break

                # Process the result
                self.response(result, job)

                break  # success, exit loop
            
            except Exception as e:
                retries += 1
                print(f"[_run_job DEBUG] Job {job} failed with exception: {e}, retry {retries}")
                if retries >= max_retries:
                    print(f"[_run_job DEBUG] Job {job} exceeded max retries, skipping")
                    break
            finally:
                self._semaphore.release()

    def submit_all(self, jobs: List[Dict[str, Any]]):
        """Submit all jobs at once and wait for completion."""

        # Submit all jobs to the executor
        futures = [self._execution.submit(self._run_job, job) for job in jobs]

        # Wait for all to complete
        for future in futures:
            try:
                future.result()  # blocks until job finishes
            except Exception as e:
                # Just log the exception; do not reference local thread variables
                print(f"[WorkerPool] Job raised an exception: {e}")

    def shutdown(self, wait: bool = True):
        """Stop and close the thread pool executor."""
        self._execution.shutdown(wait=wait)

if __name__ == "__main__":

    import time
    import datetime
    import threading
    from concurrency.network.client_http import HttpClient
    from data_layer.concurrency.orchestration.spot_throttle import spot_throttle, SpotWeights
    from concurrency.orchestration.autoscaler import Autoscaler
    from concurrency.orchestration.pool import WorkerPool
    from concurrency.orchestration.jobs import job_generator, build_kline_request, parse_dates

    print("Running Pool test...")
    start_time = datetime.datetime.now()

    # --- Transport ---
    hosts = ["https://api1.binance.com", "https://api.binance.com",  "https://api2.binance.com"]
    http = HttpClient(hosts=hosts, max_retries=3, connection_timeouts_s=3, read_timeouts_s=2)

    # --- Throttle ---
    throttle = spot_throttle(max_weight_minute=5999, window_s=60)

    # --- Result collection & worker tracking ---
    results = []
    results_lock = threading.Lock()
    rtts = []
    
    def append_result(res, job):
        results.append((job['start_min'], job['end_min'], res.elapsed_ms))
        rtts.append(res.elapsed_ms)
    
    # Process results with a lock to avoid workers conflict
    def process_result(res, job):
        with results_lock:
            append_result(res, job)
    
    results.sort(key=lambda x: x[0])  # x[0] is start_min

    # --- Autoscaler ---
    autoscaler = Autoscaler(throttle=throttle, min_workers= 1, max_workers= 20)


    # --- Jobs ---
    symbol = "BTCUSDT"
    interval = "1m"
    start_date = "2023-01-01 00:00"
    end_date   = "2025-01-01 00:00"

    start_min = parse_dates(start_date)
    end_min = parse_dates(end_date)
    jobs = list(job_generator(symbol, interval, start_min, end_min, per_request_limit=1000))


    max_threads = 20
    
    print("Warm up batch starting... ")
    print(" ")
    
    warmup_jobs = 25
    warmup_batch, remaining = jobs[:warmup_jobs], jobs[warmup_jobs:] 
    
    def run_warmup(batch):
        rtt = []
        for job in batch:
            throttle.acquire(SpotWeights.KLINES)
            path, params = build_kline_request(job)
            res = http.get(path, params=params)
            rtt.append(res.elapsed_ms)
            append_result(res, job)
        return np.mean(rtt) 
    
    rtt_s = run_warmup(warmup_batch)
    target_workers = min(autoscaler.workers(rtt_s), max_threads) 
    
    print("Warm up batch ended")
    print(" ")
    
    # --- Initialize WorkerPool ---
    pool = WorkerPool(
        http= http,
        throttle= throttle,
        request_builder= build_kline_request,
        response= process_result,
        weight_per_request= SpotWeights.KLINES,
        max_threads= max_threads,
        initial_concurrency= target_workers
    )

    print(f"Remaining batches: {len(remaining)}")
    print("Workers pool starting...")
    print(" ")

    # --- Run jobs ---
    pool.submit_all(remaining)
    pool.shutdown()

    end_time = datetime.datetime.now()
    delta = end_time - start_time

    print(" ")
    print("Workers pool ended")
    print(" ")

    # --- Stats ---
    total_klines = len(results)
    avg_rtt = sum(rtts)/len(rtts) if rtts else 0.0
    throughput = total_klines / delta.total_seconds() if delta.total_seconds() > 0 else 0.0

    print("\nAll jobs completed!")
    print(" ")
    print(f"Average round time trip over {warmup_jobs} jobs is {rtt_s:.2f} seconds")
    print(f"Number of target workers based on the average RTT is {target_workers}")
    print(f"Total time: {delta}")
    # print(f"Total jobs processed: {total_klines}")
    print(f"Total klines retrieved: {total_klines}")
    print(f"Throughput: {int(throughput)} jobs/sec")
