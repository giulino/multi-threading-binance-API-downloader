from .duckdb_connect import connect_spot
import pandas as pd

symbol = "BTCUSDT" 
interval = "5m"

parquet_file = f"/Users/giuliomannini/Hedge-Room/data/binance/spot/ohlc/{interval}/{symbol}/{symbol}_{interval}_20240101-0000_20250101-0000.parquet"
database_path = "/Users/giuliomannini/Hedge-Room/data/binance/spot/ohlc/duckdb/spot_data.duckdb"


def check_data(parquet_file, database_path, interval_ms):
    
    with connect_spot(database_path) as connect: 

        rows = connect.execute(f"SELECT COUNT(*) FROM read_parquet(?)", [parquet_file]).fetchone()[0]

        # check data consistency
        df = connect.execute(f"""
                        WITH data AS (
                           SELECT DISTINCT open_time, close_time
                           from read_parquet(?)
                        ),
                        expected AS (
                          SELECT range AS tx
                          FROM range(
                              (SELECT MIN(open_time) FROM data),
                              (SELECT MAX(close_time) FROM data) + ?,
                              ?
                          )
                        )
                        
                        SELECT tx 
                        FROM expected
                        EXCEPT
                        SELECT open_time FROM data
                        ORDER BY tx  
                        """, [parquet_file, interval_ms, interval_ms]).df()
        
        # Convert from milliseconds to ISO 8601 format
        df['tx'] = pd.to_datetime(df['tx'], unit='ms')
    
    return {
        "Total rows:": rows,
        "Missing timestamps": df
    }

result = check_data(parquet_file, database_path, 300000)
print(result)
