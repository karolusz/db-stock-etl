# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest End of Day prices and volumes
# MAGIC This notebook reads end-of-day prices from a EOD web api and upserts the data to the brozne layer.
# MAGIC
# MAGIC The following parameters need to be provided:
# MAGIC - Stock Exchange 
# MAGIC - Ticker
# MAGIC - source API, default is `eodhd` (EOD Historical Data API)
# MAGIC

# COMMAND ----------

from pyspark import SparkContext, pandas
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from utils.dataloaders import loader_selector
from utils.utils import initialize_table
from utils.schemas import SCHEMAS
from delta import DeltaTable
from datetime import date, timedelta
from config import CONFIG

# COMMAND ----------

def main(spark: SparkSession):
    # Parse inputs to the notebook
    dbutils.widgets.text("exchange", "")
    dbutils.widgets.text("api", "eodhd")

    exchange = dbutils.widgets.get("exchange")
    tickers = CONFIG.get(exchange).get("tickers")
    api = dbutils.widgets.get("api")

    sink_path = f"/FileStore/stocks/raw/{exchange}"
    schema = SCHEMAS.get("STOCKS").get("raw_all")

    # initialize the target table
    initialize_table(
        spark=spark, table_path=sink_path, schema=schema, partition_by=["Ticker"]
    )

    # Get latest dates in the sink df
    # needed for proper api calls and table updates
    last_dates = (
        spark.read.load(path=sink_path).groupBy("Ticker").agg({"Date": "max"}).collect()
    )
    if len(last_dates) > 0:
        last_dates = {d["Ticker"]: d["max(Date)"] for d in last_dates}

    last_date_default = date(2023, 1, 1)

    # Configure data loader and load data
    loader = loader_selector(api)(spark)
    pds = []
    for ticker in tickers:
        if ticker in last_dates:
            max_date = last_dates[ticker]
        else:
            max_date = last_date_default

        today_date = date.today()

        # Todays data already in the dataset: skip
        if today_date <= max_date:
            continue
        pd = loader.load_data(
            exchange=exchange,
            ticker=ticker,
            date_from=str(max_date + timedelta(days=1)),
            date_to=str(today_date),
        )
        # No new data available in the API: skip
        if pd is None:
            continue
        pd["Ticker"] = ticker
        pds.append(pd)

    if len(pds) != 0:
        pd = pandas.concat(pds, ignore_index=True)
    else:  # No new data, return early
        return None

    # Merge into sink
    DeltaTable.forPath(spark, sink_path).alias("sink").merge(
        pd.to_spark().alias("updates"),
        "sink.Date = updates.Date" + " AND sink.Ticker = updates.Ticker",
    ).whenNotMatchedInsertAll().execute()

    return None

# COMMAND ----------

if __name__ == "__main__":
    main(spark=spark)
