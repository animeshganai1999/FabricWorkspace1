# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
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

# ## Delete Existing Tables
# This script can be used for clean up purposes and is commented out by default to prevent accidental deletion of existing tables. If you need to remove existing tables, please uncomment the below script before executing it.
# To uncomment the script, simply remove the # symbol at the beginning of each line of the script. Ensure you review the script carefully before running it to avoid any unintended data loss.

# CELL ********************

# Uncomment in case you need to drop the tables
# tables_to_delete = [
#     "ApparelProduct",
#     "ApparelProductType",
#     "Brand",
#     "CollectionRetailProduct",
#     "Color",
#     "ConstructionType",
#     "Country",
#     "DayOfWeek",
#     "Document",
#     "DocumentType",
#     "Fabric",
#     "Fiber",
#     "Gender",
#     "InventoryLocationOnHandBalance",
#     "Item",
#     "ProductCategory",
#     "ProductCharacteristic",
#     "ProductCharacteristicType",
#     "ProductDocument",
#     "ProductGender",
#     "ProductGroup",
#     "ProductGroupProduct",
#     "ProductRelationshipType",
#     "ProductType",
#     "RelatedProduct",
#     "RetailCollection",
#     "RetailProduct",
#     "Size",
#     "Style"
# ]
# for table in tables_to_delete:
#     result = spark.sql(f"DROP TABLE IF EXISTS {table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Tables
# 
# The provided code snippet defines a retail_model class that creates tables in a Spark environment. Existing tables will be appended.

# CELL ********************

import pyspark.sql.types as T
from delta.tables import DeltaTable
import concurrent.futures

# Set Spark configuration
spark.conf.set("spark.sql.caseSensitive", True)

# Define the path for the tables
tables_path = "Files/"

class RetailModel:
    def __init__(self, path, spark):
        self.path = path
        self.spark = spark

    def create_table(self, schema, table_name):
        try:
            # Check if the table exists
            if self.spark._jsparkSession.catalog().tableExists(table_name):
                # Append data to the existing table
                df = self.spark.createDataFrame(data=[], schema=schema)
                df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name)
                print(f"Data appended to table {table_name} successfully.")
            else:
                # Create the table and write data if it does not exist
                df = self.spark.createDataFrame(data=[], schema=schema)
                df.write.format("delta").mode("overwrite").saveAsTable(table_name)
                print(f"Table {table_name} created and data written successfully.")
        except Exception as e:
            print(f"Failed to create or append data to table {table_name}: {e}")

    def create_entities(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._create_ApparelProduct()),
                executor.submit(self._create_ApparelProductType()),
                executor.submit(self._create_Brand()),
                executor.submit(self._create_CollectionRetailProduct()),
                executor.submit(self._create_Color()),
                executor.submit(self._create_ConstructionType()),
                executor.submit(self._create_Country()),
                executor.submit(self._create_DayOfWeek()),
                executor.submit(self._create_Document()),
                executor.submit(self._create_DocumentType()),
                executor.submit(self._create_Fabric()),
                executor.submit(self._create_Fiber()),
                executor.submit(self._create_Gender()),
                executor.submit(self._create_InventoryLocationOnHandBalance()),
                executor.submit(self._create_Item()),
                executor.submit(self._create_ProductCategory()),
                executor.submit(self._create_ProductCharacteristic()),
                executor.submit(self._create_ProductCharacteristicType()),
                executor.submit(self._create_ProductDocument()),
                executor.submit(self._create_ProductGender()),
                executor.submit(self._create_ProductGroup()),
                executor.submit(self._create_ProductGroupProduct()),
                executor.submit(self._create_ProductRelationshipType()),
                executor.submit(self._create_ProductType()),
                executor.submit(self._create_RelatedProduct()),
                executor.submit(self._create_RetailCollection()),
                executor.submit(self._create_RetailProduct()),
                executor.submit(self._create_Size()),
                executor.submit(self._create_Style()),
            ]
            concurrent.futures.wait(futures)

    def _create_ApparelProduct(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("ThreadCount", T.IntegerType(), True),
            T.StructField("StyleId", T.StringType(), True),
            T.StructField("FabricHandId", T.StringType(), True),
            T.StructField("DyeTypeId", T.StringType(), True),
            T.StructField("CoatingId", T.StringType(), True),
            T.StructField("WeaveTypeId", T.StringType(), True),
            T.StructField("ConstructionTypeId", T.StringType(), True),
            T.StructField("FiberId", T.StringType(), True),
            T.StructField("FabricId", T.StringType(), True),
            T.StructField("ApparelProductTypeId", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ApparelProduct")

    def _create_ApparelProductType(self):
        schema = T.StructType([
            T.StructField("ApparelProductTypeId", T.StringType(), False),
            T.StructField("ApparelProductTypeName", T.StringType(), True),
            T.StructField("ApparelProductTypeDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ApparelProductType")

    def _create_Brand(self):
        schema = T.StructType([
            T.StructField("BrandId", T.StringType(), False),
            T.StructField("BrandName", T.StringType(), True),
            T.StructField("BrandDescription", T.StringType(), True),
            T.StructField("BrandMark", T.BinaryType(), True),
            T.StructField("BrandTrademark", T.BinaryType(), True),
            T.StructField("BrandLogo", T.BinaryType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Brand")

    def _create_CollectionRetailProduct(self):
        schema = T.StructType([
            T.StructField("RetailCollectionId", T.StringType(), False),
            T.StructField("RetailProductId", T.StringType(), False),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "CollectionRetailProduct")

    def _create_Color(self):
        schema = T.StructType([
            T.StructField("ColorId", T.StringType(), False),
            T.StructField("ColorName", T.StringType(), True),
            T.StructField("ColorDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Color")

    def _create_ConstructionType(self):
        schema = T.StructType([
            T.StructField("ConstructionTypeId", T.StringType(), False),
            T.StructField("ConstructionTypeName", T.StringType(), True),
            T.StructField("ConstructionTypeDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ConstructionType")

    def _create_Country(self):
        schema = T.StructType([
            T.StructField("CountryId", T.StringType(), False),
            T.StructField("IsoCountryName", T.StringType(), True),
            T.StructField("Iso2LetterCountryCode", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Country")

    def _create_DayOfWeek(self):
        schema = T.StructType(
            [
                T.StructField("DayOfWeekId", T.StringType(), False),
                T.StructField("DayOfWeekName", T.StringType(), True),
                T.StructField("DayOfWeekDescription", T.StringType(), True),
                T.StructField("BusinessDayIndicator", T.BooleanType(), True),
                T.StructField("WeekEndIndicator", T.BooleanType(), True),
            ]
        )
        self.create_table(schema, "DayOfWeek")

    def _create_Document(self):
        schema = T.StructType([
            T.StructField("DocumentId", T.StringType(), False),
            T.StructField("DocumentTypeId", T.StringType(), False),
            T.StructField("DocumentUrl", T.StringType(), True),
            T.StructField("DocumentName", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Document")

    def _create_DocumentType(self):
        schema = T.StructType([
            T.StructField("DocumentTypeId", T.StringType(), False),
            T.StructField("DocumentTypeName", T.StringType(), True),
            T.StructField("DocumentTypeDescription", T.StringType(), True),
        ])
        self.create_table(schema, "DocumentType")

    def _create_Fabric(self):
        schema = T.StructType([
            T.StructField("FabricId", T.StringType(), False),
            T.StructField("FabricName", T.StringType(), True),
            T.StructField("FabricDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Fabric")

    def _create_Fiber(self):
        schema = T.StructType([
            T.StructField("FiberId", T.StringType(), False),
            T.StructField("FiberName", T.StringType(), True),
            T.StructField("FiberDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Fiber")

    def _create_Gender(self):
        schema = T.StructType([
            T.StructField("GenderId", T.StringType(), False),
            T.StructField("GenderName", T.StringType(), True),
            T.StructField("GenderDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Gender")

    def _create_InventoryLocationOnHandBalance(self):
        schema = T.StructType([
            T.StructField("ItemSku", T.StringType(), False),
            T.StructField("ActualItemQuantity", T.DecimalType(13, 2), True),
            T.StructField("OnlineIndicator", T.IntegerType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "InventoryLocationOnHandBalance")

    def _create_Item(self):
        schema = T.StructType([
            T.StructField("ItemSku", T.StringType(), False),
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("ItemName", T.StringType(), True),
            T.StructField("ItemDescription", T.StringType(), True),
            T.StructField("ListPrice", T.DecimalType(13, 2), True),
            T.StructField("InformationLabelText", T.StringType(), True),
            T.StructField("DescriptiveLabelText", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Item")

    def _create_ProductCategory(self):
        schema = T.StructType([
            T.StructField("ProductCategoryId", T.StringType(), False),
            T.StructField("ProductCategoryName", T.StringType(), True),
            T.StructField("ProductCategoryDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductCategory")

    def _create_ProductCharacteristic(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("ProductCharacteristicTypeId", T.StringType(), False),
            T.StructField("ProductCharacteristic", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductCharacteristic")

    def _create_ProductCharacteristicType(self):
        schema = T.StructType([
            T.StructField("ProductCharacteristicTypeId", T.StringType(), False),
            T.StructField("ProductCharacteristicTypeName", T.StringType(), True),
            T.StructField("ProductCharacteristicTypeDescription", T.StringType(), True),
        ])
        self.create_table(schema, "ProductCharacteristicType")

    def _create_ProductDocument(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("DocumentId", T.StringType(), False),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductDocument")

    def _create_ProductGender(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("GenderId", T.StringType(), False),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductGender")

    def _create_ProductGroup(self):
        schema = T.StructType([
            T.StructField("ProductGroupId", T.StringType(), False),
            T.StructField("ProductGroupName", T.StringType(), True),
            T.StructField("ProductGroupDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductGroup")

    def _create_ProductGroupProduct(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("ProductGroupId", T.StringType(), False),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductGroupProduct")

    def _create_ProductRelationshipType(self):
        schema = T.StructType([
            T.StructField("ProductRelationshipTypeId", T.StringType(), False),
            T.StructField("ProductRelationshipTypeName", T.StringType(), True),
            T.StructField("ProductRelationshipTypeDescription", T.StringType(), True),
        ])
        self.create_table(schema, "ProductRelationshipType")

    def _create_ProductType(self):
        schema = T.StructType([
            T.StructField("ProductTypeId", T.StringType(), False),
            T.StructField("ProductTypeName", T.StringType(), True),
            T.StructField("ProductTypeDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "ProductType")

    def _create_RelatedProduct(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("RelatedProductId", T.StringType(), False),
            T.StructField("ProductRelationshipTypeId", T.StringType(), False),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "RelatedProduct")

    def _create_RetailCollection(self):
        schema = T.StructType([
            T.StructField("RetailCollectionId", T.StringType(), False),
            T.StructField("RetailCollectionName", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "RetailCollection")

    def _create_RetailProduct(self):
        schema = T.StructType([
            T.StructField("ProductId", T.StringType(), False),
            T.StructField("ItemSku", T.StringType(), True),
            T.StructField("ProductTypeId", T.StringType(), True),
            T.StructField("ProductName", T.StringType(), True),
            T.StructField("ProductDescription", T.StringType(), True),
            T.StructField("PrimaryBrandId", T.StringType(), True),
            T.StructField("ProductShortDescription", T.StringType(), True),
            T.StructField("ProductModelName", T.StringType(), True),
            T.StructField("ProductBaseName", T.StringType(), True),
            T.StructField("IntroductionDate", T.DateType(), True),
            T.StructField("PlannedAbandonmentDate", T.DateType(), True),
            T.StructField("ProductNetWeight", T.DecimalType(18, 8), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "RetailProduct")

    def _create_Size(self):
        schema = T.StructType([
            T.StructField("SizeId", T.StringType(), False),
            T.StructField("SizeName", T.StringType(), True),
            T.StructField("SizeDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Size")

    def _create_Style(self):
        schema = T.StructType([
            T.StructField("StyleId", T.StringType(), False),
            T.StructField("StyleName", T.StringType(), True),
            T.StructField("StyleDescription", T.StringType(), True),
            T.StructField("CreateTs", T.TimestampType(), True),
            T.StructField("UpdateTs", T.TimestampType(), True),
        ])
        self.create_table(schema, "Style")

# Initialize the model and create the tables
model = RetailModel(path=tables_path, spark=spark)
model.create_entities()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
