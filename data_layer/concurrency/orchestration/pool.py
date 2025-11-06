
import asyncio
import inspect
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from concurrency.network.client_http import HttpClient, HttpResult
from concurrency.network.client_throttle import Throttle_, SpotWeights

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    Async job runner for Binance downloads with throttle-aware concurrency.
    """

    def __init__(
        self,
        http: HttpClient,
        throttle: Throttle_,
        request_builder: Callable[[Dict[str, Any], str], Tuple[str, Dict[str, Any]]],
        response: Callable[[HttpResult, Dict[str, Any]], Any],
        weight_per_request: int = SpotWeights.KLINES,
        max_concurrency: int = 50,
        market_type: str = "spot",
    ):
        self.http = http
        self.throttle = throttle
        self.request_builder = request_builder
        self.response = response
        self.weight = int(weight_per_request)
        self._max_concurrency = max(1, int(max_concurrency))
        self._market_type = market_type
        self._response_is_coro = inspect.iscoroutinefunction(response)

    async def _handle_response(self, result: HttpResult, job: Dict[str, Any]):
        if self._response_is_coro:
            await self.response(result, job)  # type: ignore[func-returns-value]
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.response, result, job)

    async def _run_job(
        self,
        job: Dict[str, Any],
        semaphore: asyncio.Semaphore,
        max_retries: int,
    ):
        async with semaphore:
            attempt = 0
            while attempt <= max_retries:
                attempt += 1
                try:
                    await self.throttle.acquire(self.weight)
                    path, params = self.request_builder(job, self._market_type)
                    result = await self.http.get(path, params=params)
                    await self._handle_response(result, job)
                    return
                except Exception as exc:
                    if attempt > max_retries:
                        logger.error("Job %s failed after %d attempts: %s", job, attempt - 1, exc)
                        raise
                    backoff = min(2 ** (attempt - 1), 5)
                    await asyncio.sleep(backoff)

    async def run(
        self,
        jobs: List[Dict[str, Any]],
        concurrency: Optional[int] = None,
        max_retries: int = 3,
    ):
        if not jobs:
            return

        limit = min(self._max_concurrency, len(jobs))
        if concurrency is not None:
            limit = min(limit, max(1, int(concurrency)))

        semaphore = asyncio.Semaphore(limit)
        tasks = [
            asyncio.create_task(self._run_job(job, semaphore, max_retries))
            for job in jobs
        ]

        errors: List[Exception] = []
        for task in asyncio.as_completed(tasks):
            try:
                await task
            except Exception as exc:
                errors.append(exc)

        if errors:
            raise errors[0]

    async def shutdown(self):
        """Kept for API parity; nothing to close for async workers."""
        return None


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    from concurrency.orchestration.jobs import build_kline_request, job_generator, parse_dates

    hosts = [
        "https://api.binance.com",
        "https://api-gcp.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com",
        "https://api4.binance.com"
    ]

    async with HttpClient(hosts=hosts, max_retries=3, connection_timeouts_s=3, read_timeouts_s=2) as http:
        throttle = Throttle_(max_weight_minute=5998, window_s=60, min_sleep=0.01)

        raw_rows: List[List[Any]] = []
        results_lock = asyncio.Lock()

        async def process_result(res: HttpResult, job: Dict[str, Any]):
            rows = res.data if isinstance(res.data, list) else []
            if not rows:
                return
            async with results_lock:
                raw_rows.extend(rows)

        symbol = "BTCUSDT"
        interval = "1m"
        start_date = "2023-01-01 00:00"
        end_date = "2023-01-02 00:00"

        start_min = parse_dates(start_date)
        end_min = parse_dates(end_date)
        jobs = list(job_generator(symbol, interval, start_min, end_min, per_request_limit=1000))

        max_concurrency = 50
        weight_capacity = max(1, throttle.max_weight_minute // SpotWeights.KLINES)
        target_workers = min(max_concurrency, weight_capacity, len(jobs) or 1)

        pool = WorkerPool(
            http=http,
            throttle=throttle,
            request_builder=build_kline_request,
            response=process_result,
            weight_per_request=SpotWeights.KLINES,
            max_concurrency=max_concurrency,
            market_type="spot",
        )

        await pool.run(jobs, concurrency=target_workers)

        print(f"Target concurrency: {target_workers}")
        print(f"Collected klines rows: {len(raw_rows)}")
        print(f"Average throttle usage: {throttle.mean_usage() * 100:.2f}%")


if __name__ == "__main__":
    asyncio.run(main())
