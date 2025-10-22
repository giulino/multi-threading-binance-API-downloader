import duckdb 
import polars as pl

df = pl.read_parquet("/home/homercrypton/hedge-room/data/spot/ohlc/1m/btc_1m_20200101_20251014.parquet")
connect = duckdb.connect(df)