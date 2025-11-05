# Binance Concurrent Market Data Downloader (Work in progress)

A high-performance Python framework for downloading and validating historical OHLC market data from the Binance REST API — featuring multi-threaded concurrency, rate-limit control, and blazing-fast data processing with Polars.

## TL;DR

🚀 **Download historical Binance spot and futures data efficiently and safely across all supported intervals.**  
This project implements a **concurrent downloader** that:
- Uses a **multi-threaded worker pool** to request data in parallel,  
- Dynamically measures API latency (RTT) to estimate optimal concurrency,  
- Adopts a **fixed autoscaling logic** — the optimal number of workers is computed once after a warm-up phase and used throughout,  
- Leverages **Polars** for in-memory transformations and efficient Parquet export

## Overview

The system is designed for **quantitative researchers and data engineers** who need large volumes of cryptocurrencies market data for analysis, backtesting, and modeling. 

```
                                        ┌───────────────────────────────┐
                                        │        User Input             │
                                        │ (symbol, interval, dates)     │
                                        └──────────────┬────────────────┘
                                                       │
                                                       ▼
                                        ┌───────────────────────────────┐
                                        │       Job Generator           │
                                        │ Splits date range into        │
                                        │ concurrent API requests       │
                                        └──────────────┬────────────────┘
                                                       │
                                                       ▼
                                        ┌───────────────────────────────┐
                                        │ Warm-up pool (single-thread)  │
                                        │ to calculate the round time   │
                                        │ trip after n requests and     │
                                        │ calculating the optimal number│
                                        │ of workers                    │
                                        └──────────────┬────────────────┘
                                                       │
                                                       ▼
                                        ┌───────────────────────────────┐
                                        │ Retrieve data using the       │
                                        │ calculated number of workers. │
                                        │ Optimized to use all the      │ 
                                        │ available binance weights     │
                                        └──────────────┬────────────────┘
                                                       │
                                                       ▼
                                        ┌───────────────────────────────┐
                                        │   Data Aggregation (Polars)   │
                                        │ Parses and converts           │
                                        │ timestamps and exports to     │
                                        │ Parquet format                │
                                        └──────────────┬────────────────┘

```

## Features

- Multi-threaded concurrency: worker pool architecture for fast parallel downloads
- Fixed autoscaler: optimizes performance after a warm-up phase
- Rate-limit aware: automatically respects Binance API limits and handles retries and errors
- Efficient data processing: powered by Polars for speed and memory efficiency


## Benchmark Results

Single-threaded

| Market Type  | Symbol     | Interval  |          Period         | Warm-up Requests | Workers | Duration  | RTT (avg) | Total Klines  | Throughput (Klines/sec)|
|--------------|------------|-----------|-------------------------|------------------|---------|-----------|-----------|---------------|------------------------|
| Spot         | BTCUSDT    | 1m        | 2024-01-01 → 2025-01-01 | 25               | 1       | 2m 17s    | 0.30s     | 527,041       | 3,845                  |
| USD Futures  | ETHUSDT    | 1m        | 2024-01-01 → 2025-01-01 | 25               | 1       | 4m 34s    | 0.27s     | 527,041       | 1,922                  |
| Coin Futures | BTCUSD_PERP| 1m        | 2024-01-01 → 2025-01-01 | 25               | 1       | 4m 14s    | 0.26s     | 527,041       | 2,067                  |

Multi-threading

| Market Type  | Symbol     | Interval  |          Period         | Warm-up Requests | Workers  | Duration  | RTT (avg) | Total Klines  | Throughput (lines/sec) |
|--------------|------------|-----------|-------------------------|------------------|----------|-----------|-----------|---------------|------------------------|
| Spot         | BTCUSDT    | 1m        | 2024-01-01 → 2025-01-01 | 25               | 14       | 20s       | 0.29s     | 527,041       | 26,117                 |
| USD Futures  | ETHUSDT    | 1m        | 2024-01-01 → 2025-01-01 | 25               | 14       | 32s       | 0.28s     | 527,041       | 16,079                 |
| Coin Futures | BTCUSD_PERP| 1m        | 2024-01-01 → 2025-01-01 | 25               | 13       | 1m 46s    | 0.27s     | 527,041       | 4,930                  |


_Note: Benchmark results may vary depending on hardware specifications, network latency, and system load. These figures serve as indicative performance metrics, not absolute values._




