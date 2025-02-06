# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "10784708-f581-41d8-97b0-19a9c5e93061",
# META       "default_lakehouse_name": "RDS_MCFSI_IDC_Insight_DemopRDS_LH_silver",
# META       "default_lakehouse_workspace_id": "4bcbee58-5a37-4d65-ab1c-1d439eebc4b4",
# META       "known_lakehouses": [
# META         {
# META           "id": "10784708-f581-41d8-97b0-19a9c5e93061"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Optimize and Compact Tables
# The provided code snippet is designed to optimize and compact tables in a Spark environment. It defines a function to handle the optimization and compaction process for multiple tables, making the code more concise and maintainable.

# CELL ********************

from delta import DeltaTable
from pyspark.sql.utils import AnalysisException

# ShopperCopilot tables location
tables_path = "Tables/"

def optimize_and_compact_table(spark, table_name, z_order_column):
    try:
        delta_table = DeltaTable.forPath(spark, f"{tables_path}/{table_name}").alias(table_name)
        delta_table.optimize().executeCompaction(),
        delta_table.optimize().executeZOrderBy(z_order_column),
    except AnalysisException as e:
        print(f"Skipping table {table_name}: {e}"),

tables = [
    ("ProductCharacteristic", "ProductId"),
    ("ProductDocument", "ProductId"),
    ("RetailProduct", "ProductId"),
    ("Item", "ProductId"),
    ("RelatedProduct", "ProductId"),
    ("InventoryLocationOnHandBalance", "ItemSku"),
    ("Document", "DocumentId")
]

for table_name, z_order_column in tables:
    optimize_and_compact_table(spark, table_name, z_order_column)
