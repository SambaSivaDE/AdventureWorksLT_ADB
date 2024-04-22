# Databricks notebook source
# MAGIC %run 
# MAGIC "./FunctionsNotebook"

# COMMAND ----------

display(dbutils.fs.ls("/mnt/AdventureworksLT/bronze"))
display(dbutils.fs.ls("/mnt/AdventureworksLT/silver"))
display(dbutils.fs.ls("/mnt/AdventureworksLT/gold"))

# COMMAND ----------

dbutils.fs.put('/adbIncubationLab1/IncubationLabBatch01/Config/Databricks.json', """
{
    "workspace_url": "https://adb-<########>.6.azuredatabricks.net/",
    "databricks_access_token": "<Databricks PA token>",
    "ADLS_Container_Name": "Constianer Name",  
    "SAS_Tokens": {
        "Bronze_Token": "sp=<bronze layer container token>",
        "Silver_Token": "sp=<silver layer container token>",
        "Gold_Token": "sp=<gold layer container token>",
        "Archive_Token": "sp=<archvie layer container token>",
        },
    "primary_keys_dict": {
        "Address": ["AddressID"],
        "Customer": ["CustomerID"],
        "CustomerAddress": ["AddressID", "CustomerID"],
        "Product": ["ProductID"],
        "ProductCategory": ["ProductCategoryID"],
        "ProductDescription": ["ProductDescriptionID"],
        "ProductModel": ["ProductModelID"],
        "ProductModelProductDescription": ["Culture", "ProductDescriptionID", "ProductModelID"],
        "SalesOrderDetail": ["SalesOrderDetailID", "SalesOrderID"],
        "SalesOrderHeader": ["SalesOrderID"]
    },
    "Mount_Paths": {
        "bronzePath": "/mnt/AdventureworksLT/bronze/",
        "silverPath": "/mnt/AdventureworksLT/silver/",
        "goldPath": "/mnt/AdventureworksLT/gold/",
        "archivePath": "/mnt/AdventureworksLT/archive/"
    },
    "Connection_String":"jdbc:sqlserver://<sql server name>.database.windows.net:1433;database=AdventureWorksLT;user=<username>;password=<user password>;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
}
""", True)


# COMMAND ----------

#dbutils.fs.rm("/Workspace",True)

# COMMAND ----------

from delta.tables import *

bronzeSchemaPath= bronzePath+'SalesLT/'
bronzeTables = getTables(bronzeSchemaPath)
print(bronzeTables)
for table_name in bronzeTables:
    delta_table = DeltaTable.forPath(spark, f"/mnt/AdventureworksLT/silver/SalesLT/{table_name}")
    #delta_table.optimize()
    print(table_name+'is optimized')

