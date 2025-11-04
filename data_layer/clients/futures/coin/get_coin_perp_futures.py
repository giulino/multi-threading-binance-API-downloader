import time
from datetime import datetime
from typing import Any, Dict, List, Optional
import warnings

import polars as pl
from pathlib import Path
import logging
import threading
import numpy as np

from concurrency.network.client_http import HttpClient, HttpResult
from concurrency.orchestration.pool import WorkerPool
from concurrency.network.client_throttle import Throttle_, PerpFuturesWeights
from concurrency.orchestration.autoscaler import Autoscaler
from concurrency.orchestration.jobs import (                            
    job_generator,
    build_kline_request,
    parse_dates,
    total_jobs
)

# Parse Binance kline rows into dicts
def _rows_to_dicts(symbol: str, interval: str, rows: List[List[Any]]) -> List[Dict[str, Any]]:
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
def get_klines(
    symbol: str,
    interval: str,
    start_date: str,     
    end_date: str,       
    path: Optional[str] = None,
    per_request_limit: int = 500,
):
    """
    Download Spot klines via REST, aggregate into a Polars DataFrame,
    and write a Parquet file if `path` is provided.

    Returns (df, parquet_path) where parquet_path is the written path (or
    a default suggestion if not provided).
    """
    
    start_min = parse_dates(start_date)  
    end_min   = parse_dates(end_date)

    # Create transport + throttle
    http = HttpClient(
        hosts=(
            "https://dapi.binance.com",
        ),
        connection_timeouts_s= 3.0,
        read_timeouts_s= 2.0,
        max_retries= 5,
        backoff_base_s= 0.25,
        backoff_factor= 2,
        default_headers= None
    )
    
    throttle = Throttle_(
        max_weight_minute= 5998,
        window_s= 60,
        clock= time.monotonic,
        min_sleep= 2.0
    )

    # Calculate total jobs
    total_Jobs, aligned_start, aligned_end = total_jobs(start_min, end_min, interval, per_request_limit)
    
    # Generate jobs
    jobs = list(job_generator(
        symbol= symbol,
        interval= interval,
        start_min= aligned_start,
        end_min= aligned_end,
        per_request_limit= per_request_limit,
    ))

    # Define autoscaler function
    autoscaler = Autoscaler(throttle= throttle, min_workers= 1, max_workers= 50)

    # Result collection 
    results: List[Dict[str, Any]] = []
    results_lock = threading.Lock()

    def process_result(res: HttpResult, job: Dict[str, Any]):
        rows = res.data if isinstance(res.data, list) else []
        parsed = _rows_to_dicts(job["symbol"], job["interval"], rows)
        if parsed:
            with results_lock:
                results.extend(parsed)
            logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
            logging.info(f"Processed {len(parsed)} rows for job {job['symbol']}\
                         from {job['start_min']} to {job['end_min']}")

    warmup_jobs = 25
    warmup_batch, remaining = jobs[:warmup_jobs], jobs[warmup_jobs:] 
    
    def run_warmup(batch):
        rtt = []
        for job in batch:
            throttle.acquire(PerpFuturesWeights.KLINES)
            path, params = build_kline_request(job, type = "coin_future")
            res = http.get(path, params=params)
            rtt.append(res.elapsed_ms)
            process_result(res, job)
        if len(rtt) == 0:
            warnings.warn("The warm up rtt list is empty - setting a default rtt of 0.25s")
            rtt = 0.25
        return np.mean(rtt) 
    
    max_threads = 20
    rtt_s = run_warmup(warmup_batch)
    target_workers = min(autoscaler.workers(rtt_s), max_threads)

    # Build WorkerPool
    pool = WorkerPool(
        http= http,
        throttle= throttle,
        request_builder= build_kline_request,
        response= process_result,
        weight_per_request= PerpFuturesWeights.KLINES,
        max_threads= max_threads,
        initial_concurrency= target_workers,
        market_type= "coin_future"
    )
    
    # Submit all jobs at once
    pool.submit_all(remaining)
    pool.shutdown()

    # Build polars dataframe
    if not results:
        df = pl.DataFrame(
            {
                "symbol": pl.Series([]),
                "interval": pl.Series([]),
                "open_time": pl.Series([]),
                "open": pl.Series([]),
                "high": pl.Series([]),
                "low": pl.Series([]),
                "close": pl.Series([]),
                "volume": pl.Series([]),
                "close_time": pl.Series([]),
                "quote_volume": pl.Series([]),
                "num_trades": pl.Series([]),
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
        warmup_jobs, rtt_s, target_workers,\
        total_Jobs

# Example: BTCUSDT 1m, one year
if __name__ == "__main__":
    
    start = datetime.now()

    REQUEST_LIMIT = 500

    SYMBOL = ["BTCUSD_PERP"] 
    INTERVAL = "1m"
    START = "2024-01-01 00:00"
    END   = "2025-01-01 00:00"

    
    for ticker in SYMBOL:
        
        start_time = datetime.now()

        path = f"~/hedge-room/data/binance/futures/ohlc/{INTERVAL}/{ticker}" # f"/Users/giuliomannini/Hedge-Room/data/binance/spot/ohlc/{INTERVAL}/{ticker}"

        df, parquet_path,\
        warmup_jobs, rtt_s,\
        target_workers, total_Jobs = get_klines(ticker, INTERVAL, START, END, path, per_request_limit= REQUEST_LIMIT)
        
        end_time = datetime.now()
        
        # Stats
        total_klines = len(df)
        delta = end_time - start_time
        throughput = total_klines / delta.total_seconds() if delta.total_seconds() > 0 else 0.0
        
        print(f"\nAll requests completed for {ticker}!")
        print(" ")
        if total_Jobs > warmup_jobs:
            print(f"Average round time trip over {warmup_jobs} jobs is {rtt_s:.2f} seconds")
            print(f"Number of target workers based on the average RTT is {target_workers}")
        print(f"Total time: {delta}")
        print(f"Total klines processed: {total_klines}")
        20
        print(f"Throughput: {int(throughput)} klines/sec")
        print(" ")
        print(f"Parquet file located at: {parquet_path}")
        print(df.tail(5))
    
    end = datetime.now()
    delta = end - start
    print(" ")
    print(f"All data were correctly retrieved in {delta}!")