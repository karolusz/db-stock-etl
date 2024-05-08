# Databricks notebook source
# MAGIC %md
# MAGIC ## Refine Price Data
# MAGIC This notebook cleans the raw data ingested from the API.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from typing import List, Tuple
from pyspark.sql import functions as F
from utils.alerts import moving_average as MA
import utils.business_logic as BL
from config import CONFIG

# COMMAND ----------

def negative_prices_to_nulls(df: DataFrame, column: str) -> DataFrame:
    """Sets negative prices to nulls"""

    def replace_negative(column):
        return F.when(F.col(column) > 0, F.col(column)).otherwise(F.lit(None))

    return df.withColumn(column, replace_negative(column))

# COMMAND ----------

def forward_fill_nulls(
    df: DataFrame, cols_to_fill: List[str], orderBy: str, partitionBy: str
) -> DataFrame:
    """Forward fills selected columns"""
    window = (
        Window.partitionBy(partitionBy)
        .orderBy(orderBy)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    for col in cols_to_fill:
        df = df.withColumn(col, F.last(F.col(col), ignorenulls=True).over(window))
    return df

# COMMAND ----------

def main(spark: SparkSession):
    dbutils.widgets.text("exchange", "")
    exchange = dbutils.widgets.get("exchange")

    source_path = f"/FileStore/stocks/raw/{exchange}"
    sink_path = f"/FileStore/stocks/refined/{exchange}"

    partition_col = "Ticker"
    order_col = "Date"

    df = spark.read.format("delta").load(path=source_path)

    # prices and volumes can't be negative
    measure_cols = ["Open", "Close", "High", "Low", "Volume"]
    for col in measure_cols:
        df = negative_prices_to_nulls(df, col)

    # forward fill missing prices and volumes
    for col in measure_cols:
        df = forward_fill_nulls(
            df, cols_to_fill=measure_cols, orderBy=order_col, partitionBy=partition_col
        )

    # Drop remianing NA values
    df = df.dropna()

    # TO DO: Adjust prices for splits and dividends

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "True"
    ).partitionBy("Ticker").save(path=sink_path)

    return None

# COMMAND ----------

if __name__ == "__main__":
    main(spark=spark)

# COMMAND ----------


