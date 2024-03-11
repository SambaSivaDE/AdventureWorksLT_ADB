# Databricks notebook source
# DBTITLE 1,Linking Bronze Container to Databricks
storageAccountName = "adls001ilbatch01siva"
sasToken = "sp=racwlme&st=2024-03-11T13:48:58Z&se=2024-04-30T21:48:58Z&spr=https&sv=2022-11-02&sr=c&sig=o%2FooKnI8FDrzSIXoplETMcnB%2BVj01LxoK0AJLR1tJiU%3D"
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
sasToken = "sp=racwlme&st=2024-03-11T13:51:45Z&se=2024-04-30T21:51:45Z&spr=https&sv=2022-11-02&sr=c&sig=PSUIov594%2FhHrHv%2BHsIDZ7a5EGJQvTC05fdgVoZh6AI%3D"
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
sasToken = "sp=racwlme&st=2024-03-11T13:55:21Z&se=2024-04-30T21:55:21Z&spr=https&sv=2022-11-02&sr=c&sig=thyRG3%2BRWYENB7yFlqBPzxjPclumSQXgNGtjBU4Z%2BrM%3D"
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

display(dbutils.fs.ls("/mnt/AdventureworksLT/bronze"))
display(dbutils.fs.ls("/mnt/AdventureworksLT/silver"))
display(dbutils.fs.ls("/mnt/AdventureworksLT/gold"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

sdfgkl

# COMMAND ----------

dbutils.fs.unmount('/mnt/AdventureworksLT/bronze')
dbutils.fs.unmount('/mnt/AdventureworksLT/silver')
dbutils.fs.unmount('/mnt/AdventureworksLT/gold')
