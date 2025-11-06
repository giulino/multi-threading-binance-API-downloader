import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from pathlib import Path
import logging
import numpy as np

from concurrency.network.client_http import HttpClient, HttpResult
from concurrency.orchestration.pool import WorkerPool
from concurrency.network.client_throttle import Throttle_, SpotWeights
from concurrency.orchestration.jobs import (                            
    job_generator,
    build_kline_request,
    parse_dates,
    total_jobs
)

# Parse Binance kline rows into dicts
def _rows_to_dicts(symbol: str, interval: str, rows: List[List[Any]]):
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) < 11:
            continue
        open_time_ms = int(r[0])
        close_time_ms = int(r[6])
        out.append(
            {
                "symbol": symbol,
                "interval": interval,
                "open_time": open_time_ms,  
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
                "close_time": close_time_ms,
                "quote_volume": float(r[7]),
                "num_trades": int(r[8]),
                "taker_buy_base": float(r[9]),
                "taker_buy_quote": float(r[10]),
            }
        )
    return out

def parse(df):
    """
    Convert `open_time` and `close_time` columns (seconds since epoch) 
    to ISO 8601 format strings.
    """

    for col in ["open_time", "close_time"]:
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Datetime(time_unit="ms"))      
            .alias(col)
        )
   
    df = df.with_columns(pl.col("open_time").dt.strftime("%Y-%m-%d").alias('Date'))
    df = df.with_columns(pl.col("open_time").dt.strftime("%H:%M").alias('open_time_only'))
    df = df.with_columns(pl.col("close_time").dt.strftime("%H:%M").alias('close_time_only'))
    
    df = df.drop(["open_time", "close_time"])

    return df 

# Public function
async def get_klines(
    symbol: str,
    interval: Optional[str],
    start_date: str,     # e.g. "2025-01-01 00:00"
    end_date: str,       # e.g. "2025-01-02 00:00"
    path: Optional[str] = None,
    per_request_limit: int = 1000,
):
    """
    Download Spot klines via REST, aggregate into a Polars DataFrame,
    and write a Parquet file if `path` is provided.

    Returns (df, parquet_path) where parquet_path is the written path (or
    a default suggestion if not provided).
    """
    print(f"\nFetching data for {symbol}...")

    start_min = parse_dates(start_date)
    end_min   = parse_dates(end_date)

    hosts = (
        "https://api4.binance.com",
        "https://api3.binance.com",
        "https://api2.binance.com",
        "https://api1.binance.com",
        "https://api-gcp.binance.com",
        "https://api.binance.com",
    )

    throttle = Throttle_(
        max_weight_minute=5998,
        window_s=60,
        clock=asyncio.get_running_loop().time,
        min_sleep=0.01,
    )

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
        
        jobs = list(job_generator(
            symbol=symbol,
            interval=interval,
            start_min=aligned_start,
            end_min=aligned_end,
            per_request_limit=per_request_limit,
        ))

        results: List[Dict[str, Any]] = []
        results_lock = asyncio.Lock()

        async def process_result(res: HttpResult, job: Dict[str, Any]):
            rows = res.data if isinstance(res.data, list) else []
            parsed = _rows_to_dicts(job["symbol"], job["interval"], rows)
            if not parsed:
                return
            async with results_lock:
                results.extend(parsed)

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
        await pool.shutdown()

    average_usage = throttle.mean_usage()

    # Build Polars DataFrame
    if not results:
        df = pl.DataFrame(
            {
                "symbol": pl.Series([], pl.Utf8),
                "interval": pl.Series([], pl.Utf8),
                "open_time": pl.Series([],),
                "open": pl.Series([]),
                "high": pl.Series([]),
                "low": pl.Series([]),
                "close": pl.Series([]),
                "volume": pl.Series([]),
                "close_time": pl.Series([],),
                "quote_volume": pl.Series([]),
                "num_trades": pl.Series([],),
                "taker_buy_base": pl.Series([]),
                "taker_buy_quote": pl.Series([]),
            }, 
            schema = {
                "symbol": pl.Utf8,
                "interval": pl.Utf8,
                "open_time": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "close_time": pl.Utf8,
                "quote_volume": pl.Int64,
                "num_trades": pl.Int64,
                "taker_buy_base": pl.Float64,
                "taker_buy_quote": pl.Float64,
            }
        )
    else:
        df = pl.DataFrame(results)
        df = df.sort(["open_time"])
        df = parse(df)
    
    start_label = datetime.fromisoformat(start_date).strftime("%Y%m%d-%H%M")
    end_label = datetime.fromisoformat(end_date).strftime("%Y%m%d-%H%M")

    directory = Path(path).expanduser()
    directory.mkdir(parents= True, exist_ok= True)
    out_parquet_path = directory / f"{symbol}_{interval}_{start_label}_{end_label}.parquet"

    df.write_parquet(out_parquet_path)  
    
    return df, out_parquet_path,\
        target_workers,\
        total_Jobs, average_usage

async def main():

    REQUEST_LIMIT = 1000

    SYMBOL = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    INTERVAL = "1m"
    START = "2019-01-01 00:00"
    END   = "2025-01-01 00:00"

    for ticker in SYMBOL:
        start_time = datetime.now()

        path = f"/home/homercrypton/hedge-room/data/binance/spot/ohlc/{INTERVAL}/{ticker}"

        (
            df,
            parquet_path,
            target_workers,
            total_Jobs,
            average_usage,
        ) = await get_klines(ticker, INTERVAL, START, END, path, per_request_limit=REQUEST_LIMIT)
        
        total_klines = len(df)
        end_time = datetime.now()
        delta = end_time - start_time
        throughput = total_klines / delta.total_seconds() if delta.total_seconds() > 0 else 0.0
        print(" ")
        print(f"\nAll requests completed for {ticker}!")
        print(" ")
        print(f"Total time: {delta}")
        print(f"Total klines processed: {total_klines}")
        print(f"Total requests processed: {total_Jobs}")
        print(f"Throughput: {int(throughput)} klines/sec")
        print(f"Target concurrency: {target_workers}")
        print(f"Average weight usage: {average_usage * 100:.2f}%")
        print(" ")
        print(f"Parquet file located at: {parquet_path}")
        print(df.tail(5))

if __name__ == "__main__":
    asyncio.run(main())
