# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will setup the project and all required components. <div>
# MAGIC Runtime 14.3 LTS recommended.

# COMMAND ----------

# Updates the databricsk-sdk to the latest version
%pip install databricks-sdk --upgrade
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Input

# COMMAND ----------

dbutils.widgets.text("EODHD_API_KEY", "")
EODHD_API_KEY = dbutils.widgets.get("EODHD_API_KEY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Setup Notebooks

# COMMAND ----------

dbutils.notebook.run("workspace_setup", 120, {"EODHD_API_KEY": EODHD_API_KEY})
dbutils.notebook.run("workflow_setup", 120, {"EODHD_API_KEY": EODHD_API_KEY})
