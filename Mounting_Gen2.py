# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define necessory variables

# COMMAND ----------

storage_account_name = "yourstoragename"
container_name = "yourcontainer_name"
mount_point = "yourmount_point"
client_id = "client_id "
tenant_id = "tenant_id"
client_secret ="client_secret"

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
