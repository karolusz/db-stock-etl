# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk.core import ApiClient
from databricks.sdk.errors.platform import ResourceAlreadyExists

# COMMAND ----------

def setup_secret_scope(
    client: ApiClient, scope_name: str, secret_name: str, secret_value: str
):
    # Check and create a scope
    try:
        client.do(
            "POST",
            "/api/2.0/secrets/scopes/create",
            headers={"Content-Type": "application/json"},
            body={"scope": scope_name, "scope_backend_type": "DATABRICKS"},
        )
    except ResourceAlreadyExists:
        print("Secret Scope already exists.")
    else:
        print("Scret scope created.")

    # Create a secret with API key
    client.do(
        "POST",
        "/api/2.0/secrets/put",
        headers={"Content-Type": "application/json"},
        body={"scope": scope_name, "key": secret_name, "string_value": secret_value},
    )


def set_workspace_conf(client: ApiClient, key: str, value: str):
    client.do(
        "PATCH",
        "/api/2.0/workspace-conf",
        body={key: value},
        headers={"Content-Type": "application/json"},
    )


def get_workspace_conf(client: ApiClient, key: str) -> str:
    wfs_check = client.do("GET", "/api/2.0/workspace-conf", {"keys": key})
    return wfs_check[key]


def main():
    dbutils.widgets.text("EODHD_API_KEY", "")
    EODHD_API_KEY = dbutils.widgets.get("EODHD_API_KEY")

    client = ApiClient()
    scope_name = "stocketclsecrets"
    secret_name = "eodhd_apikey"
    EODHD_API_KEY = dbutils.widgets.get("EODHD_API_KEY")

    # ----
    # SECRET SCOPE
    # ----
    setup_secret_scope(
        client=client,
        scope_name=scope_name,
        secret_name=secret_name,
        secret_value=EODHD_API_KEY,
    )

    # Check the value of the secret from the scope
    secret = dbutils.secrets.get(scope=scope_name, key=secret_name)
    assert EODHD_API_KEY == secret

    # ----
    # WOKRSPACE OPTIONS
    # workspace Files (https://docs.databricks.com/en/files/workspace.html)
    # turn on Repos
    # ----
    workspace_settings = {
        "enableWorkspaceFilesystem": "true",
        "enableDbfsFileBrowser": "true",
    }
    for key, value in workspace_settings.items():
        set_workspace_conf(client, key, value)

    for key, value in workspace_settings.items():
        read_value = get_workspace_conf(client, key)
        assert read_value == value

# COMMAND ----------

if __name__ == "__main__":
    main()
