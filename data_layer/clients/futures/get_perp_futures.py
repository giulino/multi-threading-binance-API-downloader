import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from pathlib import Path

from concurrency.network.client_http import HttpClient, HttpResult
from concurrency.orchestration.pool import WorkerPool
from concurrency.network.client_throttle import Throttle_, PerpFuturesWeights
from concurrency.orchestration.jobs import (
    job_generator,
    build_kline_request,
    parse_dates,
    total_jobs,
)


def parse(df: pl.DataFrame) -> pl.DataFrame:
    """Format timestamp columns and add readable fields."""
    df = df.with_columns(
        [
            pl.col("open_time").cast(pl.Datetime(time_unit="ms")),
            pl.col("close_time").cast(pl.Datetime(time_unit="ms")),
        ]
    )
    df = df.with_columns(
        [
            pl.col("open_time").dt.strftime("%Y-%m-%d").alias("Date"),
            pl.col("open_time").dt.strftime("%H:%M").alias("open_time_only"),
            pl.col("close_time").dt.strftime("%H:%M").alias("close_time_only"),
        ]
    )
    return df.drop(["open_time", "close_time"])


async def get_klines(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    path: Optional[str] = None,
    per_request_limit: int = 500,
) -> Any:
    """
    Download USD-margined perpetual futures klines and persist to Parquet.
    Returns (df, parquet_path, target_workers, total_jobs, avg_usage).
    """

    start_min = parse_dates(start_date)
    end_min = parse_dates(end_date)

    hosts = (
        "https://fapi.binance.com",
        "https://fapi1.binance.com",
        "https://fapi2.binance.com",
        "https://fapi3.binance.com",
    )

    throttle = Throttle_(
        max_weight_minute=5998,
        window_s=60,
        clock=asyncio.get_running_loop().time,
        min_sleep=0.01,
    )

    raw_rows: List[List[Any]] = []
    target_workers = 0

    async with HttpClient(
        hosts=hosts,
        connection_timeouts_s=3.0,
        read_timeouts_s=2.0,
        max_retries=5,
        backoff_base_s=0.25,
        backoff_factor=2,
        default_headers=None,
        pool_maxsize=128,
    ) as http:

        total_Jobs, aligned_start, aligned_end = total_jobs(start_min, end_min, interval, per_request_limit)

        jobs = list(
            job_generator(
                symbol=symbol,
                interval=interval,
                start_min=aligned_start,
                end_min=aligned_end,
                per_request_limit=per_request_limit,
            )
        )

        results_lock = asyncio.Lock()

        async def process_result(res: HttpResult, job: Dict[str, Any]):
            rows = res.data if isinstance(res.data, list) else []
            if not rows:
                return
            async with results_lock:
                raw_rows.extend(rows)

        max_concurrency = 50
        weight_capacity = max(1, throttle.max_weight_minute // PerpFuturesWeights.KLINES)
        target_workers = min(max_concurrency, weight_capacity, len(jobs) or 1)

        pool = WorkerPool(
            http=http,
            throttle=throttle,
            request_builder=build_kline_request,
            response=process_result,
            weight_per_request=PerpFuturesWeights.KLINES,
            max_concurrency=max_concurrency,
            market_type="usd_future",
        )

        await pool.run(jobs, concurrency=target_workers)
        await pool.shutdown()

    average_usage = throttle.mean_usage()

    kline_columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]

    if not raw_rows:
        df = pl.DataFrame(
            {
                "symbol": pl.Series([], pl.Utf8),
                "interval": pl.Series([], pl.Utf8),
                "open_time": pl.Series([], pl.Int64),
                "open": pl.Series([], pl.Float64),
                "high": pl.Series([], pl.Float64),
                "low": pl.Series([], pl.Float64),
                "close": pl.Series([], pl.Float64),
                "volume": pl.Series([], pl.Float64),
                "close_time": pl.Series([], pl.Int64),
                "quote_volume": pl.Series([], pl.Float64),
                "num_trades": pl.Series([], pl.Int64),
                "taker_buy_base": pl.Series([], pl.Float64),
                "taker_buy_quote": pl.Series([], pl.Float64),
            }
        )
    else:
        df = pl.DataFrame(raw_rows, schema=kline_columns)
        df = df.with_columns(
            [
                pl.col("open_time").cast(pl.Int64),
                pl.col("close_time").cast(pl.Int64),
                pl.col("num_trades").cast(pl.Int64),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
                pl.col("quote_volume").cast(pl.Float64),
                pl.col("taker_buy_base").cast(pl.Float64),
                pl.col("taker_buy_quote").cast(pl.Float64),
            ]
        ).drop("ignore")
        df = df.with_columns(
            [
                pl.lit(symbol).alias("symbol"),
                pl.lit(interval).alias("interval"),
            ]
        )
        df = df.select(
            [
                "symbol",
                "interval",
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "num_trades",
                "taker_buy_base",
                "taker_buy_quote",
            ]
        )
        df = parse(df)

    start_label = datetime.fromisoformat(start_date).strftime("%Y%m%d-%H%M")
    end_label = datetime.fromisoformat(end_date).strftime("%Y%m%d-%H%M")

    directory = Path(path).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    out_parquet_path = directory / f"{symbol}_{interval}_{start_label}_{end_label}.parquet"

    df.write_parquet(out_parquet_path)

    return df, out_parquet_path, target_workers, total_Jobs, average_usage


async def main():
    start = datetime.now()

    REQUEST_LIMIT = 500

    SYMBOL = ["ETHUSDT"]
    INTERVAL = "1m"
    START = "2024-01-01 00:00"
    END = "2025-01-01 00:00"

    for ticker in SYMBOL:
        run_start = datetime.now()
        path = f"/home/homercrypton/hedge-room/data/binance/futures/ohlc/{INTERVAL}/{ticker}"

        df, parquet_path, target_workers, total_jobs, avg_usage = await get_klines(
            ticker,
            INTERVAL,
            START,
            END,
            path,
            per_request_limit=REQUEST_LIMIT,
        )

        total_klines = len(df)
        run_end = datetime.now()
        delta = run_end - run_start
        throughput = total_klines / delta.total_seconds() if delta.total_seconds() > 0 else 0.0

        print(f"\nAll requests completed for {ticker}!")
        print(f"Total time: {delta}")
        print(f"Total klines processed: {total_klines}")
        print(f"Total requests processed: {total_jobs}")
        print(f"Throughput: {throughput:.2f} klines/sec")
        print(f"Target concurrency: {target_workers}")
        print(f"Average weight usage: {avg_usage * 100:.2f}%")
        print(f"Parquet file located at: {parquet_path}")
        print(df.tail(5))

if __name__ == "__main__":
    asyncio.run(main())
