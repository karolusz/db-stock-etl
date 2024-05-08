from databricks.sdk.core import ApiClient
from pyspark.sql import SparkSession
from config import SQLWAREHOUSE
from typing import Tuple, Dict


def warehouse_create_if_not_exists(client: ApiClient) -> Tuple[str, str]:
    """Checks if warehouse exists, creates one if it doesn't.
    Returns the warehouse ID and datasource ID."""
    sqlw_id = None
    datasource_id = None

    # Check if warehouse exists (search by name)
    warehouses = client.do(
        "GET", "/api/2.0/sql/warehouses", headers={"Content-Type": "application/json"}
    )

    for warehouse in warehouses.get("warehouses"):
        if warehouse.get("name") == SQLWAREHOUSE.get("name"):
            sqlw_id = warehouse.get("id")
            break

    # Create a new warehouse if not found
    if sqlw_id is None:
        resp = client.do(
            "POST",
            "/api/2.0/sql/warehouses",
            headers={"Content-Type": "application/json"},
            body=SQLWAREHOUSE,
        )
        sqlw_id = resp.get("id")

    # Get the data source id for the warehouse
    data_sources = client.do("GET", "/api/2.0/preview/sql/data_sources")
    for warehouse in data_sources:
        if warehouse.get("warehouse_id") == sqlw_id:
            datasource_id = warehouse.get("id")
            break

    return sqlw_id, datasource_id


def query_update_or_create(
    client: ApiClient, name: str, query: str, run_as_role: str, data_source_id: str
) -> str:
    """Creates a query, updates if exists"""
    query_id = None
    # queries = client.do("GET", "/api/2.0/preview/sql/queries", query={"q": name})
    # The above API endpoint is in preview with heavy rate restrictions!!!
    queries = {"results": []}
    # Get query id if exists
    for query in queries.get("results"):
        if query.get("name") == name:
            query_id = query.get("id")

    if query_id is None:
        url = "/api/2.0/preview/sql/queries"
    else:
        url = f"/api/2.0/preview/sql/queries/{query_id}"

    resp = client.do(
        "POST",
        "/api/2.0/preview/sql/queries",
        body={
            "data_source_id": data_source_id,
            "name": name,
            "query": query,
            "run_as_role": run_as_role,
        },
    )
    query_id = resp.get("id")

    return query_id


def alert_update_or_create(
    client: ApiClient, name: str, query_id: str, rearm: int, options: Dict
) -> str:
    """Creates a new alert or updates it if it exists."""

    alert_id = None
    # Get alert id if exists
    resp = client.do("GET", "/api/2.0/preview/sql/alerts")
    for alert in resp:
        if alert.get("name") == name:
            alert_id = alert.get("id")
            break

    # Create or update the alert
    body = {"name": name, "query_id": query_id, "rearm": rearm, "options": options}
    if alert_id is None:
        resp = client.do("POST", "/api/2.0/preview/sql/alerts", body=body)
        alert_id = resp.get("id")
    else:
        resp = client.do("PUT", f"/api/2.0/preview/sql/alerts/{alert_id}", body=body)

    return alert_id


def workflow_update_or_create(client: ApiClient, name: str, workflow: str) -> str:
    """Creates a new workflow or updates it if it exists."""
    workflow_id = None

    resp = client.do("GET", "/api/2.1/jobs/list", query={"name": name})
    if "jobs" in resp:
        workflow_id = resp.get("jobs")[0].get("job_id")

    if workflow_id is None:
        # Create a new one
        workflow_id = client.do("POST", "/api/2.1/jobs/create", body=workflow)
    else:
        # update
        client.do(
            "POST",
            "/api/2.1/jobs/update",
            body={"job_id": workflow_id, "new_settings": workflow},
        )

    return workflow_id


def get_cloud_provider(spark: SparkSession) -> str:
    """Identify the cloud provder by workspace URL"""
    worksapce_url = spark.conf.get("spark.databricks.workspaceUrl")
    cloud = "azure" if "azure" in worksapce_url else "aws"
    return cloud