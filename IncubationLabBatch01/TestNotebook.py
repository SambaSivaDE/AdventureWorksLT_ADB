# Databricks notebook source
display(dbutils.fs.ls("/mnt/AdventureworksLT/bronze"))
display(dbutils.fs.ls("/mnt/AdventureworksLT/silver"))
display(dbutils.fs.ls("/mnt/AdventureworksLT/gold"))

# COMMAND ----------

dbutils.fs.put('/adbIncubationLab1/IncubationLabBatch01/Config/Databricks.json', """
{
    "workspace_url": "https://adb-4867293382117366.6.azuredatabricks.net/",
    "databricks_access_token": "dapi009b244c3681f0b86a61da22f2af8acd-3",
    "ADLS_Container_Name": "adls001ilbatch01siva",  
    "SAS_Tokens": {
        "Bronze_Token": "sp=racwdlmeop&st=2024-03-13T12:24:24Z&se=2024-04-30T20:24:24Z&spr=https&sv=2022-11-02&sr=c&sig=WYAcxt2xZsh7sBlw4rr5mvB2S%2BZ2oh4hkX2dbEkLZjM%3D",
        "Silver_Token": "sp=racwdlmeop&st=2024-03-13T12:27:14Z&se=2024-04-30T20:27:14Z&spr=https&sv=2022-11-02&sr=c&sig=98AN5A3rSHLdSh8bJ5HDWy1X7X5UiUq8ZeHUT%2F6cNaA%3D",
        "Gold_Token": "sp=racwdlmeop&st=2024-03-13T12:28:36Z&se=2024-04-30T20:28:36Z&spr=https&sv=2022-11-02&sr=c&sig=nfD6SuoYh6uvLgqfLdZUhrrqHjsAIgJbrNFumvpu%2B10%3D",
        "Archive_Token": "sp=racwdlmeop&st=2024-03-20T06:02:25Z&se=2024-05-01T14:02:25Z&spr=https&sv=2022-11-02&sr=c&sig=vh18yrNaoRiFNEGJKAgQdgUk4DmG28prJIIFjLqFNmk%3D"
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
    }
}
""", True)


# COMMAND ----------

dbutils.fs.rm("/Workspace",True)
