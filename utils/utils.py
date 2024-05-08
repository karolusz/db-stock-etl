from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import List
from pyspark.dbutils import DBUtils


def initialize_table(
    spark: SparkSession,
    table_path: str,
    schema: StructType,
    partition_by: List = [],
) -> bool:
    """Creates an empty table at the table_path location if the table doesn't exist."""
    DeltaTable.createIfNotExists(spark).addColumns(schema).location(
        table_path
    ).partitionedBy(partition_by).execute()

    return True


def get_dbutils(spark) -> None:
    """Function to enable use of dbutils inside this module

    :param spark: spark session for which dbutils is to be imported
    return: dbutils module
    """
    return DBUtils(spark)