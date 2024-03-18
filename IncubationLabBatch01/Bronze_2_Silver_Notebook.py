# Databricks notebook source
# DBTITLE 1,Creation of Widgets
dbutils.widgets.text("LoadType", "Full")
v_LoadType = dbutils.widgets.get("LoadType")

# COMMAND ----------

v_LoadType

# COMMAND ----------

# DBTITLE 1,Run the Functions Notebook to get needed functions
# MAGIC %run 
# MAGIC "./FunctionsNotebook"

# COMMAND ----------

# DBTITLE 1,Importing Required Functions
from pyspark.sql.functions import col,lit,to_date

# COMMAND ----------

# DBTITLE 1,GetTable Name from the Bronze Container
bronzePath= "/mnt/AdventureworksLT/bronze/SalesLT/"
bronzeTables = getTables(bronzePath)
print(bronzeTables)

# COMMAND ----------

# DBTITLE 1,Performing Transformations and Writting into Silver in Delta format
for i in bronzeTables:
    bronzePath = "/mnt/AdventureworksLT/bronze/SalesLT/"+i+"/"+i+".parquet"
    print(bronzePath)
    silverPath = "/mnt/AdventureworksLT/silver/SalesLT/"+i+"/"
    df = spark.read.format("parquet").option("mode","PERMISSIVE").load(bronzePath)
    df_removedRowGUIDModifiedDate = removingRowGUIDModifiedDate(df)
    df_withOutNulls =nullHandling(df_removedRowGUIDModifiedDate)
    df_addedAuditColumns=addAuditColumns(df_withOutNulls)
    df_Final= ModifiyingTimestamp2Date(df_addedAuditColumns)
    if v_LoadType.upper() == 'FULL':
        df_Final.write.format("delta").mode("OVERWRITE").option("mergeSchema", "true").save(silverPath)
    else:
        df_Final.write.format("delta").mode("append").option("mergeSchema", "true").save(silverPath)
    print(i+ " table is load") 

# COMMAND ----------

silverAddressDF = spark.read.format("delta").load(silverPath)
display(silverAddressDF)

# COMMAND ----------

silverPath="/mnt/AdventureworksLT/silver/SalesLT/"
silverTables = getTables(silverPath)
print(silverTables)

# COMMAND ----------



# COMMAND ----------





# COMMAND ----------

productlist = [i for i in bronzeTables if 'PRODUCT' in i.upper()]
print(productlist)
for i  in productlist:
    bronzePath = "/mnt/AdventureworksLT/bronze/SalesLT/"+i+"/"+i+".parquet"
    print(bronzePath)
    df = spark.read.format("parquet").option("mode","PERMISSIVE").load(bronzePath)
    columnslist = [i for i in df.columns if "NAME" in i.upper()]
    print(columnslist)
    print(i)
    for j in columnslist:
        df=df.withColumnRenamed(j,i+j)
        df.show()

