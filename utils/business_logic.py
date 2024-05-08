from pyspark.sql import DataFrame
from typing import List, Tuple
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def previous_day_value(
    df: DataFrame, column: str, partitionBy: str = None, orderBy: str = None
) -> Tuple[DataFrame, str]:
    """Creates Previous_{column} column with data from the previous day.
    Accepts a single column or a list"""
    new_col_name = f"{column}_previous"
    window = Window.partitionBy(partitionBy).orderBy(orderBy)
    df = df.withColumn(new_col_name, F.lag(df[column]).over(window))

    return (df, new_col_name)


def calculate_change(
    df: DataFrame, new_col_name: str, current_col: str, previous_col: str
) -> DataFrame:
    """Calcualtes a percentage change between two columns"""
    return df.withColumn(
        new_col_name, (df[current_col] - df[previous_col]) / df[previous_col] * 100
    )


def calculate_simple_moving_average(
    df: DataFrame,
    target_column: str,
    periods: int,
    partitionBy: str = None,
    orderBy: str = None,
) -> Tuple[DataFrame, str]:
    """Creates a new column {traget_column}_SMA_{days} with a simple moving average of target_column with number of time periods equal to {periods}"""

    window = Window.partitionBy(partitionBy).orderBy(orderBy).rowsBetween(-periods, 0)

    return (
        df.withColumn(
            f"{target_column}_SMA_{str(periods)}", F.avg(target_column).over(window)
        ),
        f"{target_column}_SMA_{str(periods)}",
    )


def determine_crossover(
    df: DataFrame,
    trendline_col: str,
    value_col: str,
    partitionBy: str = None,
    orderBy: str = None,
) -> Tuple[DataFrame, str]:
    """Adds a column {trendline_col}_CO to indicate if the value in value_col crossed over a trendline from trendline_col"""

    window = Window.partitionBy(partitionBy).orderBy(orderBy)
    above_trend_col = f"{trendline_col}_above"
    above_trend_col_shift = f"{trendline_col}_above_shifted"
    cross_over_col = f"{trendline_col}_CO"
    df = df.withColumn(
        above_trend_col,
        F.when(F.col(value_col) >= F.col(trendline_col), 1).otherwise(-1),
    )

    df = df.withColumn(above_trend_col_shift, F.lag(above_trend_col).over(window))
    df = df.withColumn(
        cross_over_col,
        F.when(
            (
                (F.col(above_trend_col) > 0)
                & (F.col(above_trend_col) != F.col(above_trend_col_shift))
            ),
            1,
        )
        .when(
            (
                (F.col(above_trend_col) < 0)
                & (F.col(above_trend_col) != F.col(above_trend_col_shift))
            ),
            -1,
        )
        .otherwise(0),
    )
    df = df.drop(above_trend_col, above_trend_col_shift)

    return (df, cross_over_col)