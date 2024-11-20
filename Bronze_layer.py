
# COMMAND ----------

# MAGIC %md
# MAGIC #### 1st

# COMMAND ----------

df_cal = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2nd

# COMMAND ----------

df_cus = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3rd

# COMMAND ----------

df_procat = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4th

# COMMAND ----------

df_pro = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5th

# COMMAND ----------

df_ret = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6th

# COMMAND ----------

df_sales = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7th

# COMMAND ----------

df_ter = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8th

# COMMAND ----------

df_subcat = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@anishlambastoragedatalak.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans:1st

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal=df_cal.withColumn('Month',month(col('Date')))\
             .withColumn('Year',year(col('Date')))  

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Calendars")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans:2nd

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn("fullName",concat(col("Prefix"),lit(" "),col("firstName"),lit(" "),col("lastName"))).display()

# COMMAND ----------

df_cus=df_cus.withColumn("fullName", concat_ws(" ", col("Prefix"), col("firstName"), col("lastName")))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Customerss")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans:3rd

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_procat.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans:4th(AdventureWorks_Products)

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans:5th(AdventureWorks_Returns)

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans :6th(AdventureWorks_Sales)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans:7th(AdventureWorks_Territories)

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trans :8th(Product_Subcategories)

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format("parquet").mode("overwrite").save("abfss://silver@anishlambastoragedatalak.dfs.core.windows.net/Product_Subcategories")
