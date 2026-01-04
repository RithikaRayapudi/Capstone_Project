#!/usr/bin/env python
# coding: utf-8

# Imports
import pandas as pd
import numpy as np
from pathlib import Path

# Base paths (Airflow mounted volumes)
BASE_DATA_PATH = Path("/opt/airflow/data")

RAW_STOCK_PATH = BASE_DATA_PATH / "raw" / "stocks"
RAW_PORTFOLIO_PATH = BASE_DATA_PATH / "raw" / "portfolio"
PROCESSED_PATH = BASE_DATA_PATH / "processed"

PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

# Safety checks
required_stock_files = ["AAPL.csv", "MSFT.csv", "GOOGL.csv"]
for file in required_stock_files:
    if not (RAW_STOCK_PATH / file).exists():
        raise FileNotFoundError(f"Missing stock file: {RAW_STOCK_PATH / file}")

if not (RAW_PORTFOLIO_PATH / "transactions.csv").exists():
    raise FileNotFoundError("Missing portfolio transactions file")

# BRONZE → Load raw stock data
aapl = pd.read_csv(RAW_STOCK_PATH / "AAPL.csv")
msft = pd.read_csv(RAW_STOCK_PATH / "MSFT.csv")
googl = pd.read_csv(RAW_STOCK_PATH / "GOOGL.csv")

aapl["Stock"] = "AAPL"
msft["Stock"] = "MSFT"
googl["Stock"] = "GOOGL"

stocks_df = pd.concat([aapl, msft, googl], ignore_index=True)

# SILVER → Cleaning & standardization
stocks_df["Date"] = pd.to_datetime(stocks_df["Date"])
stocks_df = stocks_df.sort_values(["Stock", "Date"])

# Drop rows where all OHLC values are missing
stocks_df = stocks_df.dropna(
    subset=["Open", "High", "Low", "Close"],
    how="all"
)

# Forward-fill prices per stock
stocks_df[["Open", "High", "Low", "Close"]] = (
    stocks_df.groupby("Stock")[["Open", "High", "Low", "Close"]]
    .ffill()
)

# Fill missing volume with median
stocks_df["Volume"] = (
    stocks_df.groupby("Stock")["Volume"]
    .transform(lambda x: x.fillna(x.median()))
)

# Handle missing trading days
def reindex_trading_days(df):
    full_range = pd.date_range(
        start=df["Date"].min(),
        end=df["Date"].max(),
        freq="B"
    )
    return (
        df.set_index("Date")
          .reindex(full_range)
          .rename_axis("Date")
          .reset_index()
    )

stocks_df = (
    stocks_df.groupby("Stock", group_keys=False)
    .apply(reindex_trading_days)
)

stocks_df[["Open", "High", "Low", "Close", "Volume"]] = (
    stocks_df.groupby("Stock")[["Open", "High", "Low", "Close", "Volume"]]
    .ffill()
)

stocks_df["Stock"] = stocks_df.groupby("Stock")["Stock"].ffill()

# Feature engineering
stocks_df["Daily_Return"] = (
    stocks_df.groupby("Stock")["Close"].pct_change()
)

stocks_df["Cumulative_Return"] = (
    stocks_df.groupby("Stock")["Daily_Return"].cumsum()
)

stocks_df["MA_20"] = (
    stocks_df.groupby("Stock")["Close"]
    .transform(lambda x: x.rolling(20).mean())
)

stocks_df["MA_50"] = (
    stocks_df.groupby("Stock")["Close"]
    .transform(lambda x: x.rolling(50).mean())
)

# Price Normalization
stocks_df["Normalized_Close"] = (
    stocks_df.groupby("Stock")["Close"]
    .transform(lambda x: (x - x.min()) / (x.max() - x.min()))
)

# Save SILVER output
stocks_df.to_csv(
    PROCESSED_PATH / "cleaned_stock_data.csv",
    index=False
)

# Portfolio Transactions Cleaning (Silver)
transactions_df = pd.read_csv(RAW_PORTFOLIO_PATH / "transactions.csv")

transactions_df["Trade_Date"] = pd.to_datetime(
    transactions_df["Trade_Date"], errors="coerce"
)

transactions_df = transactions_df[
    transactions_df["Trade_Type"].isin(["BUY", "SELL"])
]

transactions_df = transactions_df.dropna(
    subset=["Trade_Date", "Stock", "Quantity", "Price"]
)

transactions_df = transactions_df[
    (transactions_df["Quantity"] > 0) &
    (transactions_df["Price"] > 0)
]

transactions_df.to_csv(
    PROCESSED_PATH / "cleaned_transactions.csv",
    index=False
)

print("✅ Bronze and Silver layers completed successfully.")
