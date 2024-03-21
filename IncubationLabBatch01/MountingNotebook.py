# Databricks notebook source
# DBTITLE 1,Run the Functions Notebook to get needed functions
# MAGIC %run 
# MAGIC "./FunctionsNotebook"

# COMMAND ----------

# DBTITLE 1,Linking Bronze Container to Databricks
storageAccountName = "adls001ilbatch01siva"
sasToken = Bronze_Token
blobContainerName = "bronze"
mountPoint = "/mnt/AdventureworksLT/bronze"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = mountPoint,
        extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# DBTITLE 1,Linking Silver Container to Databricks
storageAccountName = "adls001ilbatch01siva"
sasToken = Silver_Token
blobContainerName = "silver"
mountPoint = "/mnt/AdventureworksLT/silver"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = mountPoint,
        extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# DBTITLE 1,Linking Gold Container to Databricks
storageAccountName = "adls001ilbatch01siva"
sasToken = Gold_Token
blobContainerName = "gold"
mountPoint = "/mnt/AdventureworksLT/gold"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = mountPoint,
        extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

storageAccountName = "adls001ilbatch01siva"
sasToken = Archive_Token
blobContainerName = "archive"
mountPoint = "/mnt/AdventureworksLT/archive"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = mountPoint,
        extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

dbutils.fs.ls("/mnt/AdventureworksLT/bronze")
dbutils.fs.ls("/mnt/AdventureworksLT/silver")
dbutils.fs.ls("/mnt/AdventureworksLT/gold")
dbutils.fs.ls("/mnt/AdventureworksLT/archive")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

sdfgkl

# COMMAND ----------

# DBTITLE 1,Unmounting of bronze, silver, gold
dbutils.fs.unmount('/mnt/AdventureworksLT/bronze')
dbutils.fs.unmount('/mnt/AdventureworksLT/silver')
dbutils.fs.unmount('/mnt/AdventureworksLT/gold')
dbutils.fs.unmount('/mnt/AdventureworksLT/archive')
