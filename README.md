# Binance Async Market Data Downloader

A high-performance Python framework for downloading and validating historical OHLC market data from the Binance REST API — powered by asyncio, backpressure-aware throttling, and blazing-fast Polars data processing.

## TL;DR

🚀 **Smash through large historical pulls without touching Binance’s rate limits.**  
This project delivers:
- A fully **async HTTP/Throttle pipeline** (built on `aiohttp`)  
- A **concurrency cap** derived from Binance weight capacity  
- Lightweight result handling: raw klines cached first, parsed/vectorized later  
- **Polars** for fast DataFrame construction and Parquet export

You can find a more detailed explanation here: https://substack.com/@mannini/note/p-178069040?r=qu39u&utm_source=notes-share-action&utm_medium=web 

## Overview

Designed for **quant researchers and data engineers** who need bulk downloads for analysis, backtesting, or modeling.

```
  ┌──────────────────────────────┐
  │        User Input            │  (symbol, interval, date range)
  └──────────────┬───────────────┘
                 ▼
  ┌──────────────────────────────┐
  │       Job Generator          │  (splits range into API windows)
  └──────────────┬───────────────┘
                 ▼
  ┌──────────────────────────────┐
  │ Async WorkerPool + Throttle  │  (N concurrent coroutines, weight aware)
  └──────────────┬───────────────┘
                 ▼ 
  ┌──────────────────────────────┐
  │  Raw Row Collector (async)   │  (append raw klines only)
  └──────────────┬───────────────┘
                 ▼
  ┌──────────────────────────────┐
  │   Polars Vectorized Build    │  (cast → format → Parquet)
  └──────────────────────────────┘
```

## Features

- Async I/O concurrency: `aiohttp` + `asyncio.Semaphore`
- Backpressure-aware throttling: honors Binance weight quotas per minute
- Raw-to-Polars pipeline: low lock contention, high throughput
- Works across:
  - Spot: `data_layer/clients/spot/get_spot.py`
  - USD Perp futures: `.../futures/get_perp_futures.py`
- CLI entry points use `asyncio.run(...)`

## Benchmarks

Asyncronous

| Market       | Symbol        | Interval | Period (UTC)            | In-Flight Cap | Duration | Total Klines | Throughput |
|--------------|---------------|----------|-------------------------|---------------|----------|--------------|------------|
| Spot         | BTCUSDT       | 1m       | 2024-01-01 → 2025-01-01 |      50       | 8.5 s    | 527,041      | 61.4k/s    |
| USD Perp     | ETHUSDT       | 1m       | 2024-01-01 → 2025-01-01 |      50       | 9.4 s    | 527,041      | 55.8k/s    |

While loop

| Market       | Symbol        | Interval | Period (UTC)            | In-Flight Cap | Duration | Total Klines | Throughput |
|--------------|---------------|----------|-------------------------|---------------|----------|--------------|------------|
| Spot         | BTCUSDT       | 1m       | 2024-01-01 → 2025-01-01 |       -       | 305 s    | 527,041      |  1.7k/s    |
| USD Perp     | ETHUSDT       | 1m       | 2024-01-01 → 2025-01-01 |       -       | 313 s    | 527,041      |  1.6k/s    |

_Numbers are indicative. Actual results depend on hardware, network, and time of day._

## Project Layout

```
data_layer/
├── concurrency/
│   ├── network/
│   │   ├── client_http.py       # async HttpClient wrapper
│   │   └── client_throttle.py   # async sliding-window weight throttle
│   └── orchestration/
│       └── pool.py              # async WorkerPool (semaphore + tasks)
├── clients/
│   ├── spot/                    # spot downloader CLI
│   ├── futures/                 # USD perp downloader CLI
└── ...
```

## Quick Start

```bash

# Spot data
python -m data_layer.clients.spot.get_spot

# USD perpetual futures
python -m data_layer.clients.futures.usd.get_usd_perp_futures

# Coin perpetual futures
python -m data_layer.clients.futures.coin.get_coin_perp_futures
```

### Sample output

```
All requests completed for BTCUSDT!

Total time: 0:00:14.051236
Total klines processed: 527041
Total requests processed: 567
Throughput: 61473.49 klines/sec
Target concurrency: 50
Average weight usage: 83.4%
Parquet file located at: /…/BTCUSDT_1m_20240101-0000_20250101-0000.parquet
```

## How It Works

1. **Job generation** breaks the date range into minute windows (aligned to interval boundaries) with `per_request_limit` candles per API call.
2. **Concurrency cap** is `min(max_concurrency, throttle.max_weight_minute / weight_per_request, len(jobs))`.
3. **Async WorkerPool** kicks off tasks:
   - `await throttle.acquire(weight)`
   - `result = await http.get(...)`
   - `await process_result(result, job)` → append raw klines
4. **Throttle** ensures the weight budget renews exactly when Binance allows it. Requests beyond the budget simply await capacity.
5. After all tasks finish, the raw rows become a Polars DataFrame (vectorized casts, optional formatting) and are written to Parquet.

## Configuration Tips

- `per_request_limit=1000` (spot) or `500` (futures) already maximizes candles per call.
- Increase `max_concurrency` or `pool_maxsize` cautiously; the throttle is the ultimate limiter, but sockets and memory also matter.
- Set `min_sleep` lower (0.01s) so throttle sleep doesn’t inject unnecessary latency.
