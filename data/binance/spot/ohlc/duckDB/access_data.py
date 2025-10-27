from .duckdb_connect import connect_spot


symbol = "BTCUSDT" 
interval = "5m"

parquet_file = f"/Users/giuliomannini/Hedge-Room/data/binance/spot/ohlc/{interval}/{symbol}/{symbol}_{interval}_20240101-0000_20250101-0000.parquet"
database_path = "/Users/giuliomannini/Hedge-Room/data/binance/spot/ohlc/duckdb/spot_data.duckdb"


def count_rows(parquet_file, database_path):
    connect = connect_spot(database_path)

    df = connect.sql(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").df()

    print(df)

