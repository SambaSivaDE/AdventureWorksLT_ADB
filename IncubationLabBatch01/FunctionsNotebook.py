# Databricks notebook source
# DBTITLE 1,imports
from pyspark.sql.functions import col,lit,to_date,current_timestamp,current_date,date_format,from_utc_timestamp
from pyspark.sql.types import DateType,TimestampType
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

# DBTITLE 1,configurationDetails
import json

with open("/dbfs/adbIncubationLab1/IncubationLabBatch01/Config/Databricks.json") as f:
    configData = json.load(f)

primary_keys_dict = configData.get("primary_keys_dict")

Mount_Paths =configData.get("Mount_Paths")
bronzePath =Mount_Paths.get("bronzePath") ## /mnt/AdventureworksLT/bronze/
silverPath =Mount_Paths.get("silverPath") ## /mnt/AdventureworksLT/silver/
goldPath =Mount_Paths.get("goldPath") ## /mnt/AdventureworksLT/gold/
archivePath=Mount_Paths.get("archivePath") ## /mnt/AdventureworksLT/archive/


SAS_Tokens = configData.get("SAS_Tokens")
Bronze_Token = SAS_Tokens.get("Bronze_Token")
Silver_Token = SAS_Tokens.get("Silver_Token")
Gold_Token = SAS_Tokens.get("Gold_Token")
Archive_Token = SAS_Tokens.get("Archive_Token")



# COMMAND ----------

# DBTITLE 1,getTables
def getTables(path):
    tables= []
    for i in dbutils.fs.ls(path):
        tables.append(i.name.split('/')[0])
    return tables

# COMMAND ----------

# DBTITLE 1,addAuditColumns
def addAuditColumns(df):
  createdDateDf = df.withColumn("CreatedDate", to_date(current_date(), format="yyyy-MM-dd"))
  CreatedByDf = createdDateDf.withColumn("CreatedBy", lit("Hari"))
  ModifiedDateDf = CreatedByDf.withColumn("ModifiedDate", lit("1990-01-01").cast(DateType()))
  ModifiedByDf = ModifiedDateDf.withColumn("ModifiedBy", lit("NA"))
  return ModifiedByDf

# COMMAND ----------

# DBTITLE 1,modifiyingTimestamp2Date
def modifiyingTimestamp2Date(df):
    columnList = df.columns
    for col in columnList:
        if "Date" in col or "date" in col:
            df =df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd").cast(DateType()))
    return df


# COMMAND ----------

# DBTITLE 1,removingRowGUIDModifiedDate
def removingRowGUIDModifiedDate(df):
    ColumnsList = df.columns
    NewColumns = [Column for Column in ColumnsList if Column not in ['rowguid', 'ModifiedDate']]
    df=df.select(NewColumns)
    return df

# COMMAND ----------

# DBTITLE 1,nullHandling
def nullHandling(df):
    datatypes=df.dtypes
    stringDataTypes = [column for column,dtype in datatypes if dtype == 'string']
    intDataTypes = [column for column,dtype in datatypes if dtype == 'int']
    dateDataTypes = [column for column,dtype in datatypes if dtype == 'timestamp']
    fill_values = {}
    for col in stringDataTypes:
        fill_values[col] = 'NA'
    for col in intDataTypes:
        fill_values[col] = -1
    for col in dateDataTypes:
        fill_values[col] = '1900-01-01'
    df_filled = df.na.fill(fill_values)
    return df_filled

# COMMAND ----------

# DBTITLE 1,mergeDeltaData
def mergeDeltaData(inputDf, dbName, tableName, primaryKeys):
    inputDf.createOrReplaceTempView("input_table")
    merge_condition = " AND ".join([f"tgt.{key} = src.{key}" for key in primaryKeys])
    merge_sql = f"""
        MERGE INTO {dbName}.{tableName} AS tgt
        USING input_table AS src
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f'tgt.{col} = src.{col}' for col in inputDf.columns if col not in ["CreatedDate","CreatedBy","ModifiedDate","ModifiedBy"]] + ['ModifiedDate=current_date()', "ModifiedBy='HariIncrement'"])}
        WHEN NOT MATCHED THEN
            INSERT *
    """
    #print(merge_sql)
    spark.sql(merge_sql)

# COMMAND ----------

# DBTITLE 1,profileStats
def profileStats(df,table,dbname):
    columnsList= df.columns
    id_value = [i for i in columnsList if 'ID' in i or table in i]
    aggregate_list = []
    for i in id_value:
        aggregate_list.append(f"max({i}) as max_{i}, min({i}) as min_{i}, count(distinct {i}) as distinct_count_{i},avg({i}) as mean_{i}, var({i}) as variance_{i}, stdev({i}) as StandardDeviation_{i}")
    nullcount=[]
    for i in columnsList:
        nullcount.append(f"sum(case when {i} is Null then 1 else 0 end) as NullCount_{i}")
    select_statement = "select " + ", ".join(aggregate_list) +" , "+", ".join(nullcount)+ " from " + dbname+'.'+table
    df_profileStats=spark.sql(select_statement)
    profileStatsPath = archivePath+'profileStats/'+table+'/'+str(date.today())
    print(profileStatsPath)
    df_profileStats.coalesce(1).write.format("parquet").mode("overwrite").save(profileStatsPath)
    

# COMMAND ----------

# DBTITLE 1,Testing
#df=spark.read.parquet("/mnt/AdventureworksLT/bronze/SalesLT/Address/Address.parquet")
