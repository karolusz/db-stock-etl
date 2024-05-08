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
from utils.dataloaders import loader_selector
from utils.utils import initialize_table
from utils.schemas import SCHEMAS
from delta import DeltaTable
from datetime import date, timedelta

# COMMAND ----------

def main(spark: SparkSession, sc: SparkContext):
    # Parse inputs to the notebook
    dbutils.widgets.text("exchange", "")
    dbutils.widgets.text("ticker", "")
    dbutils.widgets.text("api", 'eodhd')

    exchange = dbutils.widgets.get("exchange")
    ticker = dbutils.widgets.get("ticker")
    api = dbutils.widgets.get("api")

    # initialize the target table
    sink_path = f"/FileStore/stocks/raw/{exchange}/{ticker}"
    schema = SCHEMAS.get("STOCKS").get("raw")
    initialize_table(spark=spark, table_path = sink_path, schema=schema)
    df_sink = spark.read.load(path = sink_path)

    # Get latest date in the sink df
    max_date = df_sink.agg({"Date": "max"}).collect()[0][0]
    if max_date is None:
        max_date = date(2023, 1, 1)

    # Get today's date and format
    # early return if todays data already in sink
    today_date = date.today()
    if today_date <= max_date:
        return None

    # Configure data loader and load data
    loader = loader_selector(api)()
    pd = loader.load_data(exchange=exchange, ticker=ticker, date_from=str(max_date+timedelta(days=1)), date_to=str(today_date))

    # no new data fetched
    if pd is None:
        return None
    # Merge with sink
    DeltaTable.forPath(spark, sink_path).alias('sink')\
        .merge(pd.to_spark().repartition(1).alias('updates'), 'sink.Date = updates.Date').whenNotMatchedInsertAll().execute()

    return None

# COMMAND ----------

if __name__ == '__main__':
    main(spark=spark, sc=sc)

# COMMAND ----------

#dbutils.fs.rm( f"/FileStore/stocks/raw/", True)


# COMMAND ----------


