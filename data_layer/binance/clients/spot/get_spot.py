from binance.concurrency.get_klines import get_klines
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