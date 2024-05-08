from pyspark.sql import types as T

SCHEMAS: dict[str, dict[str, T.StructType]] = {
    "STOCKS": {
        "raw": T.StructType(
            [
                T.StructField("Date", T.DateType(), False),
                T.StructField("Open", T.FloatType(), True),
                T.StructField("High", T.FloatType(), True),
                T.StructField("Low", T.FloatType(), True),
                T.StructField("Close", T.FloatType(), True),
                T.StructField("Volume", T.IntegerType(), True),
            ]
        ),
        "raw_all": T.StructType(
            [
                T.StructField("Ticker", T.StringType(), False),
                T.StructField("Date", T.DateType(), False),
                T.StructField("Open", T.FloatType(), True),
                T.StructField("High", T.FloatType(), True),
                T.StructField("Low", T.FloatType(), True),
                T.StructField("Close", T.FloatType(), True),
                T.StructField("Volume", T.IntegerType(), True),
            ]
        ),
    },
}