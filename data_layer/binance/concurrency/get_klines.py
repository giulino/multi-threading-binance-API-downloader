from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

import polars as pl
import os
import logging
import threading
import numpy as np

from binance.concurrency.network.client_http import HttpClient, HttpResult, HttpError, RateLimitError
from binance.concurrency.orchestration.pool import WorkerPool
from binance.concurrency.network.spot_throttle import spot_throttle, SpotWeights
from binance.concurrency.orchestration.autoscaler import Autoscaler
from binance.concurrency.orchestration.jobs import (                            
    job_generator,
    build_kline_request,
    parse_dates
)

# --- Small helper: parse Binance kline rows into dicts ---------------------------------
# Binance /api/v3/klines returns list of rows shaped like:
# [ openTime, open, high, low, close, volume, closeTime, quoteAssetVolume,
#   numberOfTrades, takerBuyBaseAssetVolume, takerBuyQuoteAssetVolume, ignore ]
def _rows_to_dicts(symbol: str, interval: str, rows: List[List[Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        # Be defensive in case of odd payloads
        if not isinstance(r, (list, tuple)) or len(r) < 11:
            continue
        open_time_ms = int(r[0])
        close_time_ms = int(r[6])
        out.append(
            {
                "symbol": symbol,
                "interval": interval,
                "open_time": open_time_ms,   # seconds (optional, handy)
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
                "close_time": close_time_ms, # seconds (optional)
                "quote_volume": float(r[7]),
                "num_trades": int(r[8]),
                "taker_buy_base": float(r[9]),
                "taker_buy_quote": float(r[10]),
            }
        )
    return out

# def parse(df):
#     """
#     Convert `open_time` and `close_time` columns (seconds since epoch) 
#     to ISO 8601 format strings.
#     """
#     # Converte columns from milliseconds to ISO 8601
#     for col in ["open_time", "close_time"]:
#         df = df.with_columns(
#             pl.col(col)
#             .cast(pl.Datetime(time_unit="ms"))      
#             # .dt.strftime("%Y-%m-%dT%H:%M")     # ISO 8601 with literal Z
#             .alias(col)
#         )
#     # Separate date and time in two columns    
#     df = df.with_columns(pl.col("open_time").dt.strftime("%Y-%m-%d").alias('Date'))
#     df = df.with_columns(pl.col("open_time").dt.strftime("%H:%M").alias('open_time_only'))
#     df = df.with_columns(pl.col("close_time").dt.strftime("%H:%M").alias('close_time_only'))
    
#     # Drop old columns
#     df = df.drop(["open_time", "close_time"])

#     return df 

# Public function
def get_klines(
    symbol: str,
    interval: str,
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

    # Convert dates in absolute minutes (UTC)
    start_min = parse_dates(start_date)  # minutes since epoch
    end_min   = parse_dates(end_date)

    # Create transport + throttle
    http = HttpClient(
        hosts=(
            "https://api4.binance.com",
            "https://api3.binance.com",
            "https://api2.binance.com",
            "https://api1.binance.com",
            "https://api-gcp.binance.com",
            "https://api.binance.com",
        ),
        connection_timeouts_s= 3.0,
        read_timeouts_s= 2.0,
        max_retries= 5,
        backoff_base_s= 0.25,
        backoff_factor= 2,
        default_headers= None
    )
    
    throttle = spot_throttle(
        max_weight_minute= 5999,
        window_s= 60,
        clock= time.monotonic,
        min_sleep= 2.0
    )
    
    # Generate jobs
    jobs = list(job_generator(
        symbol= symbol,
        interval= interval,
        start_min= start_min,
        end_min= end_min,
        per_request_limit= per_request_limit,
    ))

    # Define the autoscaler function
    autoscaler = Autoscaler(throttle= throttle, min_workers= 1, max_workers= 50)
    def scale_func(rtt_s: float):
        return autoscaler.workers(rtt_s)

    # Result collection (thread-safe)
    results: List[Dict[str, Any]] = []
    results_lock = __import__("threading").Lock()

    def process_result(res: HttpResult, job: Dict[str, Any]):
        rows = res.data if isinstance(res.data, list) else []
        parsed = _rows_to_dicts(job["symbol"], job["interval"], rows)
        if parsed:
            with results_lock:
                results.extend(parsed)
            logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
            logging.info(f"Processed {len(parsed)} rows for job {job['symbol']}\
                         from {job['start_min']} to {job['end_min']}")

    total_klines = len(jobs) * 1000

    max_threads = 20

    warmup_jobs = 25
    warmup_batch, remaining = jobs[:warmup_jobs], jobs[warmup_jobs:] 
    
    def run_warmup(batch):
        rtt = []
        for job in batch:
            throttle.acquire(SpotWeights.KLINES)
            path, params = build_kline_request(job)
            res = http.get(path, params=params)
            rtt.append(res.elapsed_ms)
            process_result(res, job)
        return np.mean(rtt) 
    
    rtt_s = run_warmup(warmup_batch)
    target_workers = min(scale_func(rtt_s), max_threads)

    # Build WorkerPool
    pool = WorkerPool(
        http= http,
        throttle= throttle,
        request_builder= build_kline_request,
        response= process_result,
        weight_per_request= SpotWeights.KLINES,
        max_threads= max_threads,
        initial_concurrency= target_workers
    )
    
    # Submit all jobs at once
    pool.submit_all(remaining)
    pool.shutdown()

    # Build Polars DataFrame + sort/dedupe --------------------------------
    if not results:
        # No data returned — empty DF with expected schema
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
        df = pl.DataFrame(results)
        # Sort by open time
        df = df.sort(["open_time"])
        # df = parse(df)
    
    start_label = datetime.datetime.fromisoformat(start_date).strftime("%Y%m%d-%H%M")
    end_label = datetime.datetime.fromisoformat(end_date).strftime("%Y%m%d-%H%M")

    directory = os.path.expanduser(path)
    os.makedirs(directory, exist_ok=True)
    out_parquet_path = os.path.join(directory, f"{symbol}_{interval}_{start_label}_{end_label}.parquet")

    df.write_parquet(out_parquet_path) # parquet format 
    
    return df , out_parquet_path,\
        total_klines, warmup_jobs,\
        rtt_s, target_workers

# Example: BTCUSDT 1m, one day
if __name__ == "__main__":
    
    import datetime

    start_time = datetime.datetime.now()

    SYMBOL = "BTCUSDT"
    INTERVAL = "1m"
    START = "2024-01-01 00:00"
    END   = "2025-01-01 00:00"
    PATH = "/home/homercrypton/hedge-room/data/binance/spot/ohlc/1m/btcusdt" 

    df, path, total_klines,\
    warmup_jobs, rtt_s,\
    target_workers= get_klines(SYMBOL, INTERVAL, START, END, PATH, per_request_limit=1000)
    
    print(f"Fetched {len(df)} rows → wrote {path}")
    
    # --- Stats ---
    total_jobs = len(df)
    end_time = datetime.datetime.now()
    delta = end_time - start_time
    throughput = total_jobs / delta.total_seconds() if delta.total_seconds() > 0 else 0.0
    
    print("\nAll jobs completed!")
    print(" ")
    print(f"Average round time trip over {warmup_jobs} jobs is {rtt_s:.2f} seconds")
    print(f"Number of target workers based on the average RTT is {target_workers}")
    print(f"Total time: {delta}")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Total klines retrieved: {total_klines}")
    print(f"Throughput: {throughput:.2f} jobs/sec")
    print(" ")
    print(f"Parquet file located at: {path}")
    print(df.tail(5))