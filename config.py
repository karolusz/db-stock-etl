CONFIG = {
    "US": {
        "tickers": [
            "NVDA",
            "TSM",
            "AVGO",
            "ASML",
            "AMD",
            "QCOM",
            "AMAT",
            "TXN",
            "INTC",
            "MU",
            "LRCX",
            "TOELY",
            "ARM",
            "ADI",
            "KLAC",
            "APH",
            "NXPI",
            "MRVL",
            "MCHP",
            "IFNNY",
            "TEL",
            "STM",
            "MRAAY",
            "MPWR",
            "RNECY",
            "ON",
            "GLW",
            "GFS",
            "FTV",
        ],
        "alerts": [
            {"type": "eod_change", "threshold": "5"},
            {"type": "eod_change", "threshold": "10"},
            {"type": "sma_crossover", "days": 20},
            {"type": "sma_crossover", "days": 200},
        ],
        "update_schedule": {
            "quartz_cron_expression": "0 10 16 ? * MON,TUE,WED,THU,FRI *",
            "timezone_id": "America/New_York",
        },
    }
}

SQLWAREHOUSE = {
    "name": "SQLServerlessEODPrices",
    "cluster_size": "X-Small",
    "min_num_clusters": "1",
    "max_num_clusters": "1",
    "auto_stop_mins": "15",
    "enable_photon": "false",
    "enable_serverless_compute": "true",
    "warehouse_type": "PRO",
}

JOB_CLUSTER = {
  "AZURE": {
        "cluster_name": "",
        "spark_version": "14.3.x-scala2.12",
        "spark_conf": {
          "spark.master": "local[*, 4]",
          "spark.databricks.cluster.profile": "singleNode"
        },
        "azure_attributes": {
          "first_on_demand": 1,
          "availability": "ON_DEMAND_AZURE",
          "spot_bid_max_price": -1
        },
        "node_type_id": "Standard_DS4_v2",
        "driver_node_type_id": "Standard_DS4_v2",
        "custom_tags": {
          "ResourceClass": "SingleNode"
        },
        "enable_elastic_disk": True,
        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
        "runtime_engine": "STANDARD",
        "num_workers": 0
      },
  "AWS": { 
        "cluster_name": "",
        "spark_version": "14.3.x-scala2.12",
        "spark_conf": {
          "spark.master": "local[*, 4]",
          "spark.databricks.cluster.profile": "singleNode"
        },
        "node_type_id": "m4.2xlarge",
        "driver_node_type_id": "m4.2xlarge",
        "custom_tags": {
          "ResourceClass": "SingleNode"
        },
        "enable_elastic_disk": True,
        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
        "runtime_engine": "STANDARD",
        "num_workers": 0
        }
}