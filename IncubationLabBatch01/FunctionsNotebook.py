# Databricks notebook source
# DBTITLE 1,imports
from pyspark.sql.functions import col,lit,to_date,current_timestamp,current_date,date_format,from_utc_timestamp
from pyspark.sql.types import DateType,TimestampType

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

def ModifiyingTimestamp2Date(df):
    columnList = df.columns
    for col in columnList:
        if "Date" in col or "date" in col:
            df =df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd").cast(DateType()))
    return df


# COMMAND ----------

# DBTITLE 1,Testing
#df=spark.read.parquet("/mnt/AdventureworksLT/bronze/SalesLT/Address/Address.parquet")

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
