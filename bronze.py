# Databricks notebook source
# MAGIC %md
# MAGIC #### Importing Necessary Libaries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronze/

# COMMAND ----------

df_cal=spark.read.format('csv')\
   .option('header','true')\
    .option('inferSchema','true')\
    .load('/mnt/bronze/AdventureWorks_Calendar')   

# COMMAND ----------

display(df_cal)