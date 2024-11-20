# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define necessory variables

# COMMAND ----------

storage_account_name = "anishlambastoragedatalak"
container_name = "bronze"
mount_point = "bronze"
client_id = "8c5c8e5e-7341-49a4-802b-d687e7baad29"
tenant_id = "6b5f1b61-1f3d-4861-a8f0-ea0991c04589"
client_secret ="N5b8Q~FiL8o78lpiUYRgwuU6k1AfNyonL4fjTbLh"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount the container

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### List contents of your mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

# MAGIC %md
# MAGIC #### List contents of mount data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronze/