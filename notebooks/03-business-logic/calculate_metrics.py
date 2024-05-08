# Databricks notebook source
# MAGIC %md
# MAGIC ## Metrics calculations.
# MAGIC
# MAGIC Calculates metrics selected by the user. Creates monitoring queries and alers.
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from typing import List, Tuple
from pyspark.sql import functions as F
from utils.alerts.moving_average import create_ma_alert
from utils.alerts.price_change import create_price_alert
import utils.business_logic as BL
from config import CONFIG

# COMMAND ----------

def add_eod_change_column(
    df: DataFrame, value_col: str, order_col: str, partition_col: str
) -> Tuple[DataFrame, str]:

    change_col_name = f"{value_col}_change"
    df, prev_col_name = BL.previous_day_value(
        df, column=value_col, orderBy=order_col, partitionBy=partition_col
    )
    df = BL.calculate_change(
        df,
        new_col_name=f"{value_col}_change",
        previous_col=prev_col_name,
        current_col=value_col,
    )

    return (df, change_col_name)

# COMMAND ----------

def eod_change_logic(
    df: DataFrame,
    threshold: str,
    order_col: str,
    partition_col: str,
    metrics_table_path: str,
    exchange: str,
) -> DataFrame:

    df, monitor_col = add_eod_change_column(
        df=df, value_col="Close", order_col=order_col, partition_col=partition_col
    )
    create_price_alert(
        metrics_table_path=metrics_table_path,
        date_col="Date",
        ticker_col="Ticker",
        change_col=monitor_col,
        threshold=threshold,
        exchange=exchange,
    )
    return df

# COMMAND ----------

def add_crossover_column(
    df: DataFrame, target_col: str, periods: int, partition_col: str, order_col=str
) -> DataFrame:

    df, ma_col = BL.calculate_simple_moving_average(
        df,
        target_column="Close",
        periods=periods,
        partitionBy=partition_col,
        orderBy=order_col,
    )
    df, co_col = BL.determine_crossover(
        df,
        trendline_col=ma_col,
        value_col="Close",
        partitionBy=partition_col,
        orderBy=order_col,
    )

    return df, co_col

# COMMAND ----------

def sma_crossover_logic(
    df: DataFrame,
    periods: int,
    partition_col: str,
    order_col: str,
    metrics_table_path: str,
    exchange: str,
) -> DataFrame:
    df, co_col = add_crossover_column(
        df=df,
        target_col="Close",
        periods=periods,
        partition_col=partition_col,
        order_col=order_col,
    )
    create_ma_alert(
        metrics_table_path=metrics_table_path,
        date_col="Date",
        ticker_col="Ticker",
        crossover_col=co_col,
        days=periods,
        exchange=exchange,
    )
    return df

# COMMAND ----------

def main(spark: SparkSession):
    dbutils.widgets.text("exchange", "")
    exchange = dbutils.widgets.get("exchange")

    source_path = f"/FileStore/stocks/refined/{exchange}"
    sink_path = f"/FileStore/stocks/metrics/{exchange}"

    partition_col = "Ticker"
    order_col = "Date"

    df = spark.read.format("delta").load(path=source_path)

    # Get previous close price and preivous date
    alerts = CONFIG.get(exchange).get("alerts")

    for alert in alerts:
        alert_type = alert.get("type")
        if alert_type == "eod_change":
            threshold = alert.get("threshold")
            df = eod_change_logic(
                df=df,
                threshold=threshold,
                partition_col=partition_col,
                order_col=order_col,
                metrics_table_path=sink_path,
                exchange=exchange
            )
        elif alert_type == "sma_crossover":
            days = alert["days"]
            df = sma_crossover_logic(
                df=df,
                periods=days,
                partition_col=partition_col,
                order_col=order_col,
                metrics_table_path=sink_path,
                exchange=exchange,
            )

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "True"
    ).partitionBy("Ticker").save(path=sink_path)

    return None

# COMMAND ----------

if __name__ == "__main__":
    main(spark=spark)
