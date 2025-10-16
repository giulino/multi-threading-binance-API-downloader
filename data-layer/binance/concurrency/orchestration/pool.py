from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
import time

from typing import Optional, Callable, Any, Tuple, Dict, List

# import project dependencies
from binance.concurrency.network.client_http import HttpClient, HttpResult, RateLimitError
from binance.concurrency.network.spot_throttle import spot_throttle, SpotWeights
from binance.concurrency.network.rtt import RoundTimeTripEMA
from binance.concurrency.orchestration.autoscaler import Autoscaler

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
    request, we update an RTT EMA and autoscale concurrency every n completetions.

    Key responsibilities
    --------------------
    - Job dispatch: pull jobs from a Queue and run them on a thread pool.
    - Concurrency control: semaphore reflects current permitted parallelism.
    - Rate limiting: call throttle.acquire(weight) *before* every request.
    - RTT feedback: feed res.elapsed_ms to a shared EMA estimator.
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
            rtt_update: Callable[[float], float],  # function to update shared RTT EMA
            request_builder: Callable[[Dict[str, Any]], Tuple[str, Dict[str, Any]]],  # builds endpoint + params
            response: Callable[[HttpResult, Dict[str, Any]], None],  # handles response
            autoscale: Callable[[float], int],  # function that decides target concurrency
            weight_per_request: int = SpotWeights.KLINES,  # Binance weight per request (default 2)
            max_threads: int = 100,  # maximum thread count
            initial_concurrency: int = 1,  # initial active workers
            autoscale_trigger: int = 10,  # autoscale trigger (every K completentions)
        ):

        # ---- Assign injected dependencies ----
        self.http = http
        self.throttle = throttle
        self.rtt_update = rtt_update
        self.request_builder = request_builder
        self.response = response
        self.autoscale = autoscale
        self.weight = int(weight_per_request)

        # ---- Internal infrastructure 
        self._execution = ThreadPoolExecutor(max_workers=int(max_threads), thread_name_prefix="wrkr")  # thread pool
        self._semaphore = threading.BoundedSemaphore(max(1, int(initial_concurrency)))  # controls concurrency dynamically

        # ---- Concurrency accounting ----
        self._lock = threading.Lock()  # protects shared counters below
        self._desired_permits = max(1, int(initial_concurrency))  # target concurrency
        self._granted_permits = max(1, int(initial_concurrency))  # currently active permits

        # ---- Autoscaling parameters ----
        self._completentions = 0  # number of finished jobs since last autoscale
        self._autoscale_trigger = int(autoscale_trigger)

        # Worker tracking
        self._active_workers = 0
        self._min_workers = float('inf')
        self._max_workers = 0
    
        # ---- Runtime state ----
        self._max_threads = int(max_threads)
        self._started = False  # ensures workers are launched only once
        self._scaled_up = False

    # ======================
    # Autoscaling Logic
    # ======================

    def check_autoscale(self, rtt_seconds: Optional[float]):
        if rtt_seconds is None:
            return
        
        workers = self.autoscale(rtt_seconds)

        delta = workers - self._granted_permits
        # DEBUG: log internal state
        print(f"[Autoscale DEBUG] RTT EMA={rtt_seconds:.3f}s | "
                f"Granted={self._granted_permits} | "
                f"Desired={self._desired_permits} | "
                f"TargetWorkers={workers} | Delta={delta}")
        if delta == 0:
            print("[Autoscale DEBUG] Delta=0, no scale change occurs")

        self.adjust_workers(workers)

    def adjust_workers(self, workers: int):
        """
        Adjust the active number of concurrent workers.
        - If new workers > current workers → immediately release extra semaphore permits (scale up)
        - If new workers < current workers → mark permits to remove gradually (scale down)
        """
        workers = max(1, min(int(workers), self._max_threads))  # clamp between 1 and max_threads
        delta = workers - self._granted_permits  # difference between target and current
        self._desired_permits = workers
        if delta > 0:
            # ---- SCALE UP ----
            for _ in range(delta):
                try:
                    self._semaphore.release()  # add new available permit
                    self._granted_permits += abs(delta)
                except ValueError:
                    break  # if semaphore is already full
            print(f"[Scale Up] Added {delta} permits, granted now={self._granted_permits}")
    
    # ======================
    # Worker Execution Loop
    # ======================

    def _run_job(self, job: Dict[str, Any], max_retries: int = 3):
        """Run a single job with throttling, RTT update, and autoscaling."""
        retries = 0
        while True:
            try:    
                print(f"[Worker DEBUG] Before acquiring semaphore: Granted={self._granted_permits}, ActiveWorkers={self._active_workers}")
                # Acquire concurrency permit
                self._semaphore.acquire()

                # Update active workers
                with self._lock:
                    self._active_workers = self._granted_permits
                    self._min_workers = min(self._min_workers, self._active_workers)
                    self._max_workers = max(self._max_workers, self._active_workers)
                
                # Pre-reserve weight in throttle
                self.throttle.acquire(self.weight)

                # Build and send request
                path, params = self.request_builder(job)
                result = self.http.get(path, params=params)

                # RTT
                try:
                    ema_ms = self.rtt_update(result.elapsed_ms)
                except Exception:
                    ema_ms = None

                # Process the result
                self.response(result, job)

                # Record completion & maybe autoscale
                with self._lock:
                    self._completentions += 1
                if self._completentions == self._autoscale_trigger:
                        with self._lock:
                           self._scaled_up = True
                           self.check_autoscale(result.elapsed_ms if ema_ms is None else ema_ms / 1000.0)
                           # print(f"[Worker DEBUG] Completed job #{self._completentions}: {job['start_min']}→{job['end_min']} | Active workers={self._granted_permits}")
                print(f"[Worker DEBUG] Completed job #{self._completentions}: {job['start_min']}→{job['end_min']} | Active workers={self._granted_permits}")

                self._semaphore.release()
                break  # success, exit loop

            except RateLimitError:
                time.sleep(0.5)
                continue  # retry same job

            except Exception as e:
                retries += 1
                print(f"[Worker] Job {job} failed with exception: {e}, retry {retries}")
                self._semaphore.release()
                if retries >= max_retries:
                    print(f"[Worker] Job {job} exceeded max retries, skipping")
                    break

    def submit_all(self, jobs: List[Dict[str, Any]]):
        """Submit all jobs at once and wait for completion."""
        if self._started:
            raise RuntimeError("WorkerPool already started")
        self._started = True

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
    from binance.concurrency.network.client_http import HttpClient
    from binance.concurrency.network.spot_throttle import spot_throttle, SpotWeights
    from binance.concurrency.network.rtt import RoundTimeTripEMA
    from binance.concurrency.orchestration.autoscaler import Autoscaler
    from binance.concurrency.orchestration.pool import WorkerPool
    from binance.concurrency.orchestration.jobs import job_generator, build_kline_request, parse_dates

    print("Running mini end-to-end WorkerPool test...")
    start_time = datetime.datetime.now()

    # --- Transport ---
    hosts = ["https://api1.binance.com", "https://api.binance.com",  "https://api2.binance.com"]
    http = HttpClient(hosts=hosts, max_retries=3, connection_timeouts_s=3, read_timeouts_s=2)

    # --- Throttle ---
    throttle = spot_throttle(max_weight_minute=5999, window_s=60)

    # --- RTT EMA ---
    rtt_ema = RoundTimeTripEMA(alpha=0.7)
    def rtt_update(elapsed_ms: float):
        return rtt_ema(elapsed_ms)

    # --- Result collection & worker tracking ---
    results = []
    results_lock = threading.Lock()
    _active_workers = 0
    _min_workers = float('inf')
    _max_workers = 0
    active_lock = threading.Lock()
    rtts = []

    def process_result(res, job):
        global _active_workers, _min_workers, _max_workers
        with active_lock:
            _active_workers = pool._granted_permits
            _min_workers = min(_min_workers, _active_workers)
            _max_workers = max(_max_workers, _active_workers)
        with results_lock:
            results.append((job['start_min'], job['end_min'], res.elapsed_ms))
            rtts.append(res.elapsed_ms)
    
    results.sort(key=lambda x: x[0])  # x[0] is start_min
    # Now `results` is guaranteed to be in chronological order
    for r in results:
        start_min, end_min, elapsed_ms = r
        print(f"{start_min}→{end_min}, RTT={elapsed_ms:.2f} ms")
        # print(f"Processed job {job['start_min']}→{job['end_min']} ms RTT={res.elapsed_ms:.2f} | Active workers: {_active_workers}")

    # --- Autoscaler ---
    autoscaler = Autoscaler(throttle=throttle, min_workers= 1, max_workers= 20)
    def scale_func(rtt_s: float):
        return autoscaler.workers(rtt_s)

    # --- Jobs ---
    symbol = "BTCUSDT"
    interval = "5m"
    start_date = "2024-01-01 00:00"
    end_date   = "2025-01-01 00:00"

    start_min = parse_dates(start_date)
    end_min = parse_dates(end_date)
    jobs = list(job_generator(symbol, interval, start_min, end_min, per_request_limit=1000))

    print(f"Submitting {len(jobs)} jobs...")

    # --- Initialize WorkerPool ---
    pool = WorkerPool(
        http= http,
        throttle= throttle,
        rtt_update= rtt_update,
        request_builder= build_kline_request,
        response= process_result,
        autoscale= scale_func,
        weight_per_request= SpotWeights.KLINES,
        max_threads= 20,
        initial_concurrency= 1,
        autoscale_trigger= 10
    )

    # --- Run jobs ---
    pool.submit_all(jobs)
    pool.shutdown()

    end_time = datetime.datetime.now()
    delta = end_time - start_time

    # --- Stats ---
    total_jobs = len(results)
    avg_rtt = sum(rtts)/len(rtts) if rtts else 0.0
    throughput = total_jobs / delta.total_seconds() if delta.total_seconds() > 0 else 0.0

    print("\nAll jobs completed!")
    print(f"Total Time: {delta}")
    print(f"Total Jobs Processed: {total_jobs}")
    print(f"Min workers used: {_min_workers}")
    print(f"Max workers used: {_max_workers}")
    print(f"Average RTT: {avg_rtt:.2f} ms")
    print(f"Throughput: {throughput:.2f} jobs/sec")
    # print(f"Results: {results}")
