-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "a698a4d0-77e5-4e28-9a2b-251cf90dd7a9",
-- META       "default_lakehouse_name": "CrossTenantADLS",
-- META       "default_lakehouse_workspace_id": "02f507b1-5567-4d4d-a587-069858caf7e6",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "a698a4d0-77e5-4e28-9a2b-251cf90dd7a9"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE my_table (
    id INT,
    name STRING,
    age INT
);

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "synapse_pyspark"
-- META }
