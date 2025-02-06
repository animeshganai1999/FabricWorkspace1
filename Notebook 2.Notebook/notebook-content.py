# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7e5c0a6f-88cf-4a53-9463-0a6ca7699af3",
# META       "default_lakehouse_name": "lh_Compliance",
# META       "default_lakehouse_workspace_id": "4bcbee58-5a37-4d65-ab1c-1d439eebc4b4",
# META       "known_lakehouses": [
# META         {
# META           "id": "7e5c0a6f-88cf-4a53-9463-0a6ca7699af3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

pip install azure-kusto-data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AAD_TENANT_ID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
KUSTO_CLUSTER = "https://s360prodro.kusto.windows.net"
KUSTO_DATABASE = "service360db"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

KCSB = KustoConnectionStringBuilder.with_aad_device_authentication(
    KUSTO_CLUSTER)
KCSB.authority_id = AAD_TENANT_ID

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

KUSTO_CLIENT = KustoClient(KCSB)
KUSTO_QUERY = "StormEvents | sort by StartTime desc | take 10"

RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
