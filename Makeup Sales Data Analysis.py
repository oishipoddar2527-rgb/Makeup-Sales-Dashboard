# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Beauty Sales").getOrCreate()

df = spark.read.csv("/Volumes/workspace/default/makeup/Cosmetic_products_sales.csv", header=True, inferSchema=True)

df.show()



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

df.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]
).show()

# COMMAND ----------

print(df.count())

# COMMAND ----------

df= df.dropDuplicates()

# COMMAND ----------

df=df.withColumn(
    "Zone",
    upper(col("Zone"))
)

# COMMAND ----------

df.createOrReplaceTempView("sales")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct Zone from sales where month(Date)=1 and year(Date)=2018

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC SUM(`Net Sales calculated`) AS Revenue
# MAGIC FROM sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ParentSKU,
# MAGIC SUM(Qty) AS Units,
# MAGIC SUM(`Net Sales calculated`) AS Revenue
# MAGIC FROM sales
# MAGIC GROUP BY ParentSKU
# MAGIC ORDER BY Revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC Zone,
# MAGIC SUM(`Net Sales calculated`) AS Revenue
# MAGIC FROM sales
# MAGIC GROUP BY Zone
# MAGIC ORDER BY Revenue DESC

# COMMAND ----------

monthly_sales = spark.sql("""

SELECT

ParentSKU,

Year,

Month,

Zone,

SUM(Qty) AS Total_Qty,

SUM(`Net Sales calculated`) AS Revenue

FROM sales

GROUP BY
ParentSKU,
Year,
Month,
Zone
ORDER BY
Year,
Month

""")

monthly_sales.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM beauty_sales_monthly
# MAGIC LIMIT 10;

# COMMAND ----------

monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("beauty_sales_monthly")