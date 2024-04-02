# Databricks notebook source
# DBTITLE 1,Creation of Widgets
dbutils.widgets.text("LoadType", "Full")
v_loadType = dbutils.widgets.get("LoadType")
dbutils.widgets.text("SchemaName", "SalesLT")
v_schemaName = dbutils.widgets.get("SchemaName")
dbutils.widgets.text("Tables", "ALL")
v_tablesList = dbutils.widgets.get("Tables")

# COMMAND ----------

# DBTITLE 1,Validation of Widgets
v_schemaName+' '+v_loadType

# COMMAND ----------

# DBTITLE 1,Execute necessary functions
# MAGIC %run 
# MAGIC "./FunctionsNotebook"

# COMMAND ----------

# DBTITLE 1,Importing Required Functions
from pyspark.sql.functions import col,lit,to_date

# COMMAND ----------

# DBTITLE 1,GetTable Name from the Bronze Container
bronzeSchemaPath= bronzePath+v_schemaName+'/'
silverSchemaPath= silverPath+v_schemaName+'/'
bronzeTables = getTables(bronzeSchemaPath)
#print(bronzeTables)

# COMMAND ----------

# DBTITLE 1,Performing Transformations and Writting into Silver in Delta format
for i in bronzeTables:
    bronzeTablePath = bronzeSchemaPath+i+"/"+i+".parquet"
    print(bronzeTablePath)
    silverPath = silverSchemaPath+i+"/"
    df = spark.read.format("parquet").option("mode","PERMISSIVE").load(bronzeTablePath)
    df_removedRowGUIDModifiedDate = removingRowGUIDModifiedDate(df)
    df_withOutNulls =nullHandling(df_removedRowGUIDModifiedDate)
    df_addedAuditColumns=addAuditColumns(df_withOutNulls)
    df_Final= modifiyingTimestamp2Date(df_addedAuditColumns)
    if v_loadType.upper() == 'FULL':
        df_Final.write.format("delta").mode("OVERWRITE").option("mergeSchema", "true").save(silverPath)
    else:
        for tableName,primaryKeys in primary_keys_dict.items() :
            if tableName == i:
                mergeDeltaData(df_Final, "silver_saleslt", tableName, primaryKeys)
                print(tableName+ " is merged")
    profileStats(df_Final,i,"silver_saleslt")
    print(i+ " table is load") 
