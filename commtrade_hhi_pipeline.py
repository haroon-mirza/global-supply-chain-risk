import os

import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()

## Config
api_key = os.getenv("COMTRADE_API_KEY")
reporters = "842,276,392,410,156"
commodities = "8542,8471,30,2709,7108,7601,8507,8541"
period = "2025"


## Requests Helper Function
def fetch_json(url):
    print(f"Fetching {url[:60]}...")
    response = requests.get(url, timeout=40)
    response.raise_for_status()
    return response.json()


## Extract
def extract_data():
    print("Fetching Trades...")
    trade_url = (
        "https://comtradeapi.un.org/data/v1/get/C/A/HS"
        f"?reporterCode={reporters}"
        f"&period={period}"
        f"&cmdCode={commodities}"
        f"&flowCode=M"
        f"&subscription-key={api_key}"
    )
    trade_data = fetch_json(trade_url)

    print("Fetching Dimensions....")
    reporter_data = fetch_json("https://comtradeapi.un.org/files/v1/app/reference/Reporters.json")
    com_data = fetch_json("https://comtradeapi.un.org/files/v1/app/reference/HS.json")
    partner_data = fetch_json("https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json")

    return trade_data, reporter_data, com_data, partner_data


## Transform
def transform_data(trade_data, reporter_data, com_data, partner_data):
    print("Building Dimension Tables...")
    
    # reporter_dim table
    reporter_dim = pl.DataFrame(reporter_data["results"]).rename({"text": "reporter_name"})

    # com_dim table
    com_dim = pl.DataFrame(com_data["results"]).rename({"text": "commodity_name"})
    com_list = commodities.split(",")
    com_dim = com_dim.filter(pl.col("id").is_in(com_list))
    com_dim = com_dim.with_columns(
        pl.col("commodity_name").str.split(" - ").list.last().alias("commodity_name")
    )

    # partner_dim table
    partner_dim = pl.DataFrame(partner_data["results"])
    partner_dim = partner_dim.with_columns(
        pl.col('text').replace({'Other Asia, nes': 'Taiwan'})
    )

    # trades_raw table
    print("Building Trades Table...")
    trades_raw = pl.DataFrame(trade_data["data"], infer_schema_length=None)

    print("Calculating HHI...")
    hhi_df = (
        trades_raw
        .join(partner_dim, left_on='partnerCode', right_on='id', how='left')
        .filter((pl.col('isGroup') == False) & (pl.col('partnerCode') != 0))
        .group_by(['reporterCode', 'cmdCode'])
        .agg(
            ((pl.col('primaryValue') / pl.col('primaryValue').sum()) ** 2).sum().alias('hhi')
        )
        .with_columns(
            (pl.col('hhi') * 100).round(2).alias('hhi_pct'),
            pl.when(pl.col('hhi') < 0.15).then(pl.lit('Low'))
              .when(pl.col('hhi') < 0.25).then(pl.lit('Moderate'))
              .otherwise(pl.lit('High'))
              .alias('risk_tier')
        )
        .sort('hhi_pct', descending=True)
    )

    # Final Report
    final_report = (
        hhi_df
        .join(reporter_dim, left_on='reporterCode', right_on='id', how='left')
        .join(com_dim, left_on='cmdCode', right_on='id', how='left')
        .select(['reporter_name', 'commodity_name', 'hhi_pct', 'risk_tier'])
        .sort('hhi_pct', descending=True)
    )

    # Return all the final DataFrames so the Load step can save them
    return reporter_dim, com_dim, partner_dim, trades_raw, final_report


## 3. Load
def load_data(reporter_dim, com_dim, partner_dim, trades_raw, final_report):
    print("Saving to Parquet...")
    reporter_dim.write_parquet('raw/reporter_dim.parquet')
    com_dim.write_parquet('raw/com_dim.parquet')
    partner_dim.write_parquet('raw/partner_dim.parquet')
    trades_raw.write_parquet('raw/trades_raw.parquet')
    
    print(final_report.head(10))
    final_report.write_parquet('marts/hhi_summary.parquet')
    print("Pipeline complete")


## Main
if __name__ == "__main__":
    # Extract
    t_data, rep_data, c_data, part_data = extract_data()
    
    # Transform
    r_dim, c_dim, p_dim, tr_raw, f_report = transform_data(t_data, rep_data, c_data, part_data)
    
    # Load
    load_data(r_dim, c_dim, p_dim, tr_raw, f_report)