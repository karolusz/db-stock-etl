from abc import ABC, abstractmethod
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from utils.apis import client
from utils.utils import get_dbutils

SECRET_SCOPE = "stocketclsecrets"
EODHD_API_KEY = "eodhd_apikey"


class StockDataLoader(ABC):
    """Interface for stock data loaders"""

    @abstractmethod
    def load_data(
        self, exchange: str, ticker: str, date_from: str = None, date_to: str = None
    ) -> DataFrame:
        pass


class EODHDDataLoader(StockDataLoader):
    def __init__(self, spark: SparkSession):
        api_token = get_dbutils(spark).secrets.get(
            scope=SECRET_SCOPE, key=EODHD_API_KEY
        )
        self._client = client.EODHDClient(api_token=api_token, fmt="csv")

    def load_data(
        self, exchange: str, ticker: str, date_from: str, date_to: str
    ) -> DataFrame:
        ticker = f"{ticker.upper()}.{exchange.upper()}"
        resp_text = self._client.get_eod_data(
            ticker=ticker, date_from=date_from, date_to=date_to
        )
        # empty response contains `Value`
        if "Value\n" in resp_text:
            return None

        resp_2d = [x.split(",") for x in resp_text.split("\n")]
        resp_2d.pop()  # removes the last empty row

        pd = DataFrame(data=resp_2d[1:], columns=resp_2d[0])

        return pd


def loader_selector(api: str) -> StockDataLoader:
    if api == "eodhd":
        return EODHDDataLoader