from databricks.sdk.core import ApiClient
from utils.project_setup import (
    warehouse_create_if_not_exists,
    query_update_or_create,
    alert_update_or_create,
)
from utils.apis.client import db_client


def create_price_alert(
    metrics_table_path: str,
    date_col: str,
    ticker_col: str,
    change_col: str,
    threshold: str,
    exchange: str,
    query_name: str = None,
    alert_name: str = None,
):
    """Creates the Moving Average alert and all needed components"""

    # Get SQL warehouse details
    warehouse_id, datasource_id = warehouse_create_if_not_exists(client=db_client)

    if query_name is None:
        query_name = f"{exchange}_{change_col}_{threshold}"

    # Update or create a query
    query_text = _query(
        metrics_table_path=metrics_table_path,
        date_col=date_col,
        ticker_col=ticker_col,
        change_col=change_col,
        threshold=threshold,
        exchange=exchange,
    )
    query_id = query_update_or_create(
        client=db_client,
        name=query_name,
        query=query_text,
        run_as_role="owner",
        data_source_id=datasource_id,
    )

    # Update or create an Alert
    if alert_name is None:
        alert_name = f"Alert - {query_name}"

    alert_subject = _custom_alert_subject(exchange=exchange, threshold=threshold)

    alert_body = _custom_alert_body(threshold=threshold)

    alert_options = {
        "run_as_role": "owner",
        "notify_on_ok": False,
        "custom_body": alert_body,
        "custom_subject": alert_subject,
        "op": "!=",
        "column": ticker_col,
        "display_column": ticker_col,
        "aggregation": None,
        "value": "NULL",
        "query_plan": None,
        "empty_result_state": "unknown",
    }
    alert_id = alert_update_or_create(
        client=db_client,
        name=alert_name,
        query_id=query_id,
        rearm=100,
        options=alert_options,
    )


def _custom_alert_subject(exchange: str, threshold: int):
    return f"{exchange} - {threshold}% change."


def _custom_alert_body(threshold: int) -> str:
    return f"""The following tickers prices changed more than  {threshold}% today:
<br>
{{{{QUERY_RESULT_TABLE}}}}
<br>
The associated query page URL: {{{{QUERY_URL}}}}"""


def _query(
    metrics_table_path: str,
    date_col: str,
    threshold: str,
    ticker_col: str,
    change_col: str,
    exchange: str,
):
    return f"""
SELECT
  `{ticker_col}`, CAST(`{change_col}` AS DECIMAL(10,3)) AS `{change_col}`
FROM
  `delta`.`{metrics_table_path}`
WHERE `{date_col}`=(SELECT MAX(`{date_col}`) FROM `delta`.`{metrics_table_path}`)
    AND
    ABS({change_col}) > {threshold}
"""