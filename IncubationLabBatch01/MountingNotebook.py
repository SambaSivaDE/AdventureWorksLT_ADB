# Databricks notebook source
# DBTITLE 1,Linking Bronze Container to Databricks
storageAccountName = "adls001ilbatch01siva"
sasToken = "sp=racwdlmeop&st=2024-03-13T12:24:24Z&se=2024-04-30T20:24:24Z&spr=https&sv=2022-11-02&sr=c&sig=WYAcxt2xZsh7sBlw4rr5mvB2S%2BZ2oh4hkX2dbEkLZjM%3D"
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
sasToken = "sp=racwdlmeop&st=2024-03-13T12:27:14Z&se=2024-04-30T20:27:14Z&spr=https&sv=2022-11-02&sr=c&sig=98AN5A3rSHLdSh8bJ5HDWy1X7X5UiUq8ZeHUT%2F6cNaA%3D"
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
sasToken = "sp=racwdlmeop&st=2024-03-13T12:28:36Z&se=2024-04-30T20:28:36Z&spr=https&sv=2022-11-02&sr=c&sig=nfD6SuoYh6uvLgqfLdZUhrrqHjsAIgJbrNFumvpu%2B10%3D"
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

dbutils.fs.ls("/mnt/AdventureworksLT/bronze")
dbutils.fs.ls("/mnt/AdventureworksLT/silver")
dbutils.fs.ls("/mnt/AdventureworksLT/gold")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

sdfgkl

# COMMAND ----------

# DBTITLE 1,Unmounting of bronze, silver, gold
dbutils.fs.unmount('/mnt/AdventureworksLT/bronze')
dbutils.fs.unmount('/mnt/AdventureworksLT/silver')
dbutils.fs.unmount('/mnt/AdventureworksLT/gold')
