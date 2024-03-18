# Databricks notebook source
# DBTITLE 1,Execute necessary functions
# MAGIC %run 
# MAGIC "./FunctionsNotebook"

# COMMAND ----------

# DBTITLE 1,Create silver_saleslt database
spark.sql("""
          Create database if not exists silver_saleslt
          """)

# COMMAND ----------

# DBTITLE 1,getTable Names from Silver layer
silverPath="/mnt/AdventureworksLT/silver/SalesLT/"
silverTables = getTables(silverPath)
print(silverTables)

# COMMAND ----------

# DBTITLE 1,Create the tables in silver_saleslt database
for i in silverTables:
    table_name = f"silver_saleslt.{i}"
    location = f"/mnt/AdventureworksLT/silver/SalesLT/{i}"
    sql_statement = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING DELTA
    LOCATION '{location}'
    """
    spark.sql(sql_statement)
    print(i + " Table is registered in silver_saleslt database")

