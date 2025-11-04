from math import ceil
from typing import Dict, Tuple, Any
from datetime import datetime, timezone

# ---- Intervals in MINUTES ----
INTERVAL_MIN: Dict[str, int] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
}

# Alignment date in minutes
def align_dates(start_min: int, end_min: int, interval: str):
    """
    Function aligning [start_min, end_min) to kline open boundaries for `interval`.
    Returns (aligned_start_min, aligned_end_min), end exclusive.

    Example
      - interval = '5m'  → each candle = 5 minutes
      - start_min = 26297283
      - end_min   = 26297311

    Then:
      - aligned_start = floor(26297283 / 5) * 5 = 26297280
      - aligned_end   = ceil(26297311 / 5) * 5  = 26297315

    So the final aligned range is:
      [26297280, 26297315)

    Meaning:
      Download data starting at minute 26297280 and 
      ending just before minute 26297315 — perfectly matching 
      the 5-minute kline grid with no overlaps or missing candles.
    """
    step = INTERVAL_MIN[interval]
    aligned_start = (start_min // step) * step
    aligned_end = ceil(end_min / step) * step           

    return aligned_start, aligned_end

# UTC minutes since epoch
def parse_dates(date_str: str):
    """ 
    Convert a timezone-aware (or naive) datetime into the total number of **minutes since the Unix epoch**.  
    
    This function returns the elapsed time between that reference point 
    and the given datetime `dt`, expressed in **whole minutes** (integer).
    """

    dt = datetime.fromisoformat(date_str)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() // 60) 

# Calculate total jobs
def total_jobs(
    start_min: int,
    end_min: int,
    interval: str,
    per_request_limit: int = 1000,
):
    """
    How many API requests (jobs) are required, based on minutes-only math.
    """

    start, end = align_dates(start_min, end_min, interval)

    step = INTERVAL_MIN[interval]
    total_candles = max(0, (end - start) // step)
    jobs = ceil(total_candles / per_request_limit)
    
    return jobs, start, end

# Job generator: one yielded dict == one REST call (one job)
def job_generator(
    symbol: str,
    interval: str,
    start_min: int,   # minutes since epoch (UTC)
    end_min: int,     # minutes since epoch (UTC), end-exclusive
    per_request_limit: int = 1000,
):
    """
    Generate sequential API job windows for Binance kline data retrieval.

    This function splits a large historical download range — defined by 
    [start_min, end_min) in **absolute minutes since epoch (UTC)** — into
    smaller, contiguous segments of up to `per_request_limit` candles each.
    Each generated segment corresponds to one REST API request window.

    The generator yields one job dictionary at a time, with the structure:
        {
            "symbol": <str>,           # e.g. "BTCUSDT"
            "interval": <str>,         # e.g. "1m", "1h"
            "start_min": <int>,        # inclusive start (in minutes since epoch)
            "end_min": <int>,          # exclusive end   (in minutes since epoch)
            "limit": <int>,            # max candles per API call
        }

    Each job covers a half-open interval [start_min, end_min), ensuring:
      • No gaps (continuous coverage across time)
      • No overlaps (no duplicate candles between jobs)
      • Perfect alignment to the chosen kline interval
    """

    step = INTERVAL_MIN[interval]
    
    start, end = align_dates(start_min, end_min, interval)

    chunk = per_request_limit * step
    
    cursor = start
    while cursor < end:
        next_ = min(cursor + chunk, end)
        yield {
            "symbol": symbol,
            "interval": interval,
            "start_min": cursor,   
            "end_min": next_,  
            "limit": per_request_limit,
        }
        cursor = next_

# Request builder
def build_kline_request(job: Dict[str, Any], type:str):
    """
    Turn a minute-based job into a Binance REST request in ms.
    """
    start_ms = int(job["start_min"]) * 60_000
    end_ms = int(job["end_min"]) * 60_000
    
    if type == "spot":
        path = "/api/v3/klines"
    elif type == "usd_future":
        path = "/fapi/v1/klines"
    elif type == "coin_future":
        path = "/dapi/v1/klines"
        
    return (
        path,
        {
            "symbol": job["symbol"],
            "interval": job["interval"],
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": job.get("limit", 1000),
        },
    )
                
if __name__ == "__main__":
    import pprint

    print("Testing jobs.py...")

    symbol = "BTCUSDT"
    interval = "1h"               
    start_date = "2025-01-01 00:00"
    end_date   = "2025-01-01 06:30"

    # Convert to absolute minutes since epoch (UTC)
    start_min = parse_dates(start_date)
    end_min   = parse_dates(end_date)

    print(f"Start minutes: {start_min}, End minutes: {end_min}")

    # Count total jobs
    jobs_needed, aligned_start, aligned_end = total_jobs(start_min, end_min, interval, per_request_limit=1)
    print(f"Total jobs needed (limit=1 candle per job): {jobs_needed}")
    print(f"Aligned range: start: {aligned_start} → end: {aligned_end} minutes")

    # Generate jobs
    for i, job in enumerate(job_generator(
        symbol=symbol,
        interval=interval,
        start_min=aligned_start,
        end_min=aligned_end,
        per_request_limit=1,
    ), start=1):
        path, params = build_kline_request(job, type="usd_future")
        print(f"Job {i}: {job['start_min']}→{job['end_min']} min | request path: {path}")
        pprint.pprint(params)

        
        
        
        
        
        
        
        
        
