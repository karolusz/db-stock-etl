import datetime
from typing import List
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from enum import Enum
from databricks.sdk.core import ApiClient


class HTTPRequestMethod(Enum):
    get = "get"
    post = "post"


class APIClient:
    """
    Base class for interacting with HTTP APIs with some basic retry config.
    Can be used to inherit functionality from and create API specific clients.
    """

    def __init__(
        self,
        default_timeout: float = 5,
        retries: int = 5,
        backoff_factor: int = 1,
        allowed_methods: List[str] = ["GET"],
        status_forcelist: List[int] = [429, 500, 502, 503, 504],
    ) -> None:
        self._retry_strategy = Retry(
            total=retries,
            status_forcelist=status_forcelist,
            allowed_methods=allowed_methods,
            backoff_factor=backoff_factor,
        )
        self.default_timeout = default_timeout
        self._adapter = HTTPAdapter(max_retries=self._retry_strategy)
        self._session = Session()
        self._session.mount("https://", self._adapter)

    @property
    def default_timeout(self) -> int:
        return self._default_timeout

    @default_timeout.setter
    def default_timeout(self, value: int):
        if value > 60:
            raise ValueError("Set a timeout of less than 60 seconds")
        self._default_timeout = value

    def send_request(self, method: str, url: str, **kwargs) -> Response:
        """
        Sends a get request to specified url, if the request is not successfull raises an error else return Response object
        """
        method = HTTPRequestMethod(method)
        # Check if timeout passed else set default timeout
        timeout = (
            kwargs.pop("timeout") if kwargs.get("timeout") else self.default_timeout
        )
        response = getattr(self._session, method.value)(
            url=url, timeout=timeout, **kwargs
        )
        response.raise_for_status()

        return response


class EODHDClient(APIClient):
    """API Client class for EOD Historical Data - EOD API"""

    def __init__(self, api_token: str, fmt: str):
        super().__init__()
        self._api_token = api_token
        self.base_url = "https://eodhd.com/api/"
        self._fmt = fmt

    @property
    def api_token(self) -> str:
        return "****"

    @api_token.setter
    def api_token(self, value: str):
        self._api_token = value

    @property
    def fmt(self) -> str:
        return self._fmt

    @fmt.setter
    def fmt(self, value: str):
        allowed_values = ["csv", "json"]
        if value not in allowed_values:
            raise ValueError(f"Format must be one of: {allowed_values}")
        self._fmt = value

    def get_eod_data(self, ticker: str, date_from: str, date_to: str):
        """
        Get EOD historical data
        https://eodhd.com/financial-apis/api-for-historical-data-and-volumes#Yahoo_Finance_API_Support
        """
        for date in [date_from, date_to]:
            self._validate_date_format(date)

        url = self.base_url + "eod/" + ticker
        params = {"api_token": self._api_token}
        params["period"] = "d"
        params["order"] = "d"
        params["fmt"] = self._fmt
        params["from"] = date_from
        params["to"] = date_to

        resposne = self.send_request("get", url, **{"params": params})
        return resposne.text

    def _validate_date_format(self, date: str) -> bool:
        """Validates that the date string is in ISO format"""

        try:
            datetime.date.fromisoformat(date)
        except ValueError:
            raise ValueError(
                f"Date proivded: {date} is invalid. Expecting ISO format YYYY-MM-DD."
            )

        return True


db_client = ApiClient()