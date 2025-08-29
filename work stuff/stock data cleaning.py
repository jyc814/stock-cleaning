import pandas as pd
from pathlib import Path

file_path = Path("C:\\stuff\\New folder\\15_years_stock_data.csv")

df_raw = pd.read_csv(file_path)

# Cleaning headers & removing duplicate columns

df = df_raw.loc[:, ~pd.Index(df_raw.columns).duplicated()].copy()
df.columns = (pd.Index(df.columns).astype(str).str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace("[\u200b\u200c\u200d\u2060]+", "", regex=True)
                .str.replace(r"[./]", " ", regex=True)
                .str.strip()
                .str.title()) # Turns 'close_aapl' -> 'Close_Aapl'

# Parsing/sorting dates

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date"]).sort_values("Date")

# Identifying price columns

close_like = [c for c in df.columns if c.lower().startswith("close")]
adj_close_like = [c for c in df.columns if c.lower().startswith("adj close")]
price_cols = close_like if close_like else adj_close_like
for c in price_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Reshaping the data set from a wide format to a long format

long = df[["Date"] + price_cols].melt(id_vars="Date", var_name="Field", value_name="Close")

# Extracting ticker from "Field"

if long["Field"].str.contains("_").any():
    tickers = long["Field"].str.split("_", n=1, expand=True)[1]
else:
    tickers = long["Field"].str.split(" ", n=1, expand=True)[1]
long["Ticker"] = tickers.fillna(long["Field"].str.replace("Close", "", case=False)).str.strip().str.upper()
long = long.dropna(subset=["Close"])

# Filtering date range to 2019-2024

start, end = pd.Timestamp("2019-01-01"), pd.Timestamp("2024-12-31")
long = long[(long["Date"] >= start) & (long["Date"] <= end)].copy()

# Aggregating to end of month closes per ticker

monthly = (long.set_index("Date")
                 .groupby("Ticker")["Close"]  
                 .resample("M").last()
                 .reset_index())

monthly["Close"] = monthly["Close"].round(2)

# Creating a wide table for 'monthly'

monthly_wide = monthly.pivot(index="Date", columns="Ticker", values="Close").sort_index()

monthly_wide.to_csv("Stocks_Monthly_Wide_2019_2024.csv")






















