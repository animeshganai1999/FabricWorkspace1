# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f5cdb80e-4a3b-4b65-b89f-5fac287c26ac",
# META       "default_lakehouse_name": "SDS_ESGDE_Sustainability_DEMO_ProcessedESGData_LH",
# META       "default_lakehouse_workspace_id": "02f507b1-5567-4d4d-a587-069858caf7e6",
# META       "known_lakehouses": [
# META         {
# META           "id": "f5cdb80e-4a3b-4b65-b89f-5fac287c26ac"
# META         },
# META         {
# META           "id": "bbac5868-0907-46be-b6fa-c5bf94bc22d2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Overview
# 
# This notebook is used for cleaning the ESG data model tables used for storing processed water, waste and emissions data from Microsoft Sustainability Manager (MSM).\
# For more information [click here](https://go.microsoft.com/fwlink/?linkid=2288320) to view ESG data estate documentation.

# MARKDOWN ********************

# ### Parameters
# 
# __CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME__ Lakehouse storing the config files.  
# __TARGET_DB__ : Name of the Lakehouse storing the processed ESG data.

# CELL ********************

CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_Sustainability_DEMO_ConfigAndDemoData_LH"
TARGET_DB = "SDS_ESGDE_Sustainability_DEMO_ProcessedESGData_LH"


# MARKDOWN ********************

# Import required libraries

# CELL ********************

import json
from notebookutils import mssparkutils

# MARKDOWN ********************

# ##### Derived parameters

# CELL ********************

config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_Sustainability_DEMO_Utilities_INTB"

# MARKDOWN ********************

# ### Initialize Configuration and Demo data for the capability

# CELL ********************

initialize_config_and_demo_data(Capability.ESGDataEstate)

# MARKDOWN ********************

# ### Delete existing processed ESG data tables
# 
# Deletes the tables from the Lakehouse which stores the processed ESG data based on the adapter files of the *ConfigAndDemoData* Lakehouse.

# CELL ********************

def ReadFile(filePath):
    rdd = spark.sparkContext.wholeTextFiles(filePath)
    return rdd.collect()[0][1]

adapter1FilePath = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/Config/TransformMSMDataToProcessedESGDataAdapter.json"
adapter2FilePath = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/Config/TransformRawImportESGDataToProcessedESGDataAdapter.json"

adapter1 = json.loads(ReadFile(adapter1FilePath))
adapter2 = json.loads(ReadFile(adapter2FilePath))

# Collect the list of anchor tables referred in the Adapter file.
anchorTables = []
for sourceTable in (adapter1['sourceTables'] + adapter2['sourceTables']):
    for anchorTable in sourceTable['targetAnchorTables']:
        if not anchorTable['tableName'] in anchorTables:
            anchorTables.append(anchorTable['tableName'])

# Delete the data from these anchor tables.
for table in spark.catalog.listTables(TARGET_DB):
    if table.name in anchorTables or table.name == "PartyLocation":
        spark.sql(f"DELETE FROM {TARGET_DB}.{table.name}")

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### Delete internal config files
# Deletes internal config files from the *configs* Lakehouse to enable capability reset and fresh start.


# CELL ********************

INTERNAL_FOLDER_PATH = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/internal/dtt"

KEY_MAPPING_FOLDER_PATH = f"{INTERNAL_FOLDER_PATH}/KEY_MAPPING"
try:
    mssparkutils.fs.rm(KEY_MAPPING_FOLDER_PATH, True)
except:
    print('Key Mapping folder path already cleaned.')

REFERENCE_MAPPING_FOLDER_PATH = f"{INTERNAL_FOLDER_PATH}/REFERENCE_MAPPING"
try:
    mssparkutils.fs.rm(REFERENCE_MAPPING_FOLDER_PATH, True)
except:
    print('Reference Mapping folder path already cleaned.')

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": true
# META }
