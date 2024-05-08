# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk.core import ApiClient
from pyspark.sql import SparkSession
from utils.project_setup import workflow_update_or_create, get_cloud_provider
from config import CONFIG, JOB_CLUSTER
from typing import Dict


def workflow_definition(
    workflow_name: str,
    exchange: str,
    job_cluster_key: str,
    repo_base_path: str,
    quartz_cron_expression: str,
    timezone_id: str,
    job_cluster_settings: Dict,
) -> Dict:
    return {
        "name": workflow_name,
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "schedule": {
            "quartz_cron_expression": quartz_cron_expression,
            "timezone_id": timezone_id,
            "pause_status": "UNPAUSED",
        },
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Ingest_data",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": f"{repo_base_path}/notebooks/01-ingestion/EOD_source_to_bronze",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": job_cluster_key,
                "libraries": [{"pypi": {"package": " databricks-sdk==0.27.0"}}],
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
                "webhook_notifications": {},
            },
            {
                "task_key": "Metrics_alerts",
                "depends_on": [{"task_key": "Refine_price_data"}],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": f"{repo_base_path}/notebooks/03-business-logic/calculate_metrics",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": job_cluster_key,
                "libraries": [{"pypi": {"package": " databricks-sdk==0.27.0"}}],
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
                "webhook_notifications": {},
            },
            {
                "task_key": "Refine_price_data",
                "depends_on": [{"task_key": "Ingest_data"}],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": f"{repo_base_path}/notebooks/02-refinement/refine_price_data",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": job_cluster_key,
                "libraries": [{"pypi": {"package": " databricks-sdk==0.27.0"}}],
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
                "webhook_notifications": {},
            },
        ],
        "job_clusters": [
            {"job_cluster_key": job_cluster_key, "new_cluster": job_cluster_settings}
        ],
        "queue": {"enabled": True},
        "parameters": [{"name": "exchange", "default": exchange}],
    }


def main(spark: SparkSession):
    client = ApiClient()

    current_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    repo_base_path = current_path.split("notebooks")[0]

    for exchange, conf in CONFIG.items():

        workflow_name = f"{exchange}-EOD-Price-Update"
        job_cluster_key = f"{exchange}-EOD-Job-Cluster"
        cron_expression = conf.get("update_schedule").get("quartz_cron_expression")
        timezone_id = conf.get("update_schedule").get("timezone_id")
        cloud = get_cloud_provider(spark)
        workflow = workflow_definition(
            workflow_name=workflow_name,
            exchange=exchange,
            job_cluster_key=job_cluster_key,
            repo_base_path=repo_base_path,
            quartz_cron_expression=cron_expression,
            timezone_id=timezone_id,
            job_cluster_settings=JOB_CLUSTER.get(cloud.upper()),
        )

        # Create or update the workflow
        workflow_id = workflow_update_or_create(
            client=client, name=workflow_name, workflow=workflow
        )
        print(f"{exchange} workflow ID: {workflow_id}")


if __name__ == "__main__":
    main(spark)

# COMMAND ----------


