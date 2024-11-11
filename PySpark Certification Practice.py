# Databricks notebook source
# MAGIC %md
# MAGIC #Environment SetUp

# COMMAND ----------

# DBTITLE 1,Dbutils List
dbutils.fs.ls("dbfs:/pyspark_practice_datasets/")

# COMMAND ----------

# DBTITLE 1,Input File Locations
events_path = "dbfs:/pyspark_practice_datasets/events/events.delta"
sales_path = "dbfs:/pyspark_practice_datasets/sales/sales.delta"
users_path = "dbfs:/pyspark_practice_datasets/users/users.delta"

# COMMAND ----------

# DBTITLE 1,Database Creation
# MAGIC %sql
# MAGIC create database practicedb;
# MAGIC use practicedb;

# COMMAND ----------

# DBTITLE 1,Tables Creation
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE events SHALLOW CLONE delta.`dbfs:/pyspark_practice_datasets/events/events.delta`;
# MAGIC CREATE OR REPLACE TABLE sales SHALLOW CLONE delta.`dbfs:/pyspark_practice_datasets/sales/sales.delta`;
# MAGIC CREATE OR REPLACE TABLE users SHALLOW CLONE delta.`dbfs:/pyspark_practice_datasets/users/users.delta`;

# COMMAND ----------

# DBTITLE 1,Dataframe Creation
events_df = spark.table("events")
events_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC #printSchema

# COMMAND ----------

events_df.printSchema()

# COMMAND ----------

events_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #Filtering
# MAGIC - Filter for rows where **`device`** is **`macOS`**
# MAGIC - Sort rows by **`event_timestamp`**

# COMMAND ----------

# DBTITLE 1,filter("device == 'macOS'")
events_df.filter("device == 'macOS'").sort("event_timestamp").display()

# COMMAND ----------

# DBTITLE 1,filter(col(device) == 'macOS')
from pyspark.sql.functions import *
events_df.filter(col("device") == 'macOS').sort("event_timestamp").display()

# COMMAND ----------

# DBTITLE 1,where("device == 'macOS'")
events_df.where("device == 'macOS'").sort("event_timestamp").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Column Expressions

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 1,col("ecommerce.purchase_revenue_in_usd")
from pyspark.sql.functions import *
revenue_df = events_df.withColumn("revenue",col("ecommerce.purchase_revenue_in_usd"))
display(revenue_df)

# COMMAND ----------

# DBTITLE 1,events_df.ecommerce.purchase_revenue_in_usd
from pyspark.sql.functions import *
revenue_df = events_df.withColumn("revenue",events_df.ecommerce.purchase_revenue_in_usd)
display(revenue_df)

# COMMAND ----------

# DBTITLE 1,events_df["ecommerce.purchase_revenue_in_usd"]
from pyspark.sql.functions import *
revenue_df = events_df.withColumn("revenue",events_df["ecommerce.purchase_revenue_in_usd"])
display(revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Filter Not NULL rows 

# COMMAND ----------

# DBTITLE 1,filter(col("revenue").isNotNull())
revenue_df.filter(col("revenue").isNotNull()).display()

# COMMAND ----------

# DBTITLE 1,where("revenue is not null")
revenue_df.where("revenue is not null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop Duplicates

# COMMAND ----------

# DBTITLE 1,select("event_name").distinct()
revenue_df.filter(col("revenue").isNotNull()).select("event_name").distinct().display()

# COMMAND ----------

# DBTITLE 1,dropDuplicates(["event_name"])
revenue_df.filter(col("revenue").isNotNull()).dropDuplicates(["event_name"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregation Methods

# COMMAND ----------

# DBTITLE 1,groupBy - count()
events_df.groupBy("event_name").count().display()

# COMMAND ----------

# DBTITLE 1,groupBy - avg()
events_df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd").display()

# COMMAND ----------

# DBTITLE 1,groupBy - sum()
events_df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd").display()

# COMMAND ----------

# DBTITLE 1,agg() Method - 1
events_df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases")).display()

# COMMAND ----------

# DBTITLE 1,agg() Method - 2
events_df.groupBy("geo.state").agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),approx_count_distinct("user_id").alias("distinct_users")).display()

# COMMAND ----------

# DBTITLE 1,Math Functions
from pyspark.sql.functions import cos, sqrt

display(spark.range(10)
        .withColumn("ceil",ceil("id"))
        .withColumn("log",log("id"))
        .withColumn("round",round("id"))
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

# MAGIC %md
# MAGIC #DateTime Functions

# COMMAND ----------

from pyspark.sql.functions import *
events_df = events_df.select("user_id", col("event_timestamp").alias("timestamp"))
events_df.display()

# COMMAND ----------

# DBTITLE 1,Convert to Timestamp - 1
events_df.withColumn("timestamp",(col("timestamp")/1e6).cast("timestamp")).display()

# COMMAND ----------

# DBTITLE 1,Convert to Timestamp - 2
from pyspark.sql.types import *
events_df = events_df.withColumn("timestamp",(col("timestamp")/1e6).cast(TimestampType()))
events_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datetime Patterns for Formatting and Parsing
# MAGIC There are several common scenarios for datetime usage in Spark:
# MAGIC
# MAGIC - CSV/JSON datasources use the pattern string for parsing and formatting datetime content.
# MAGIC - Datetime functions related to convert StringType to/from DateType or TimestampType e.g. **`unix_timestamp`**, **`date_format`**, **`from_unixtime`**, **`to_date`**, **`to_timestamp`**, etc.
# MAGIC
# MAGIC | Symbol | Meaning         | Presentation | Examples               |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | G      | era             | text         | AD; Anno Domini        |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |

# COMMAND ----------

# DBTITLE 1,dateformat
(events_df
 .withColumn("date string",date_format("timestamp","MM-dd-yyyy"))
 .withColumn("full date string",date_format("timestamp","MMMM dd, yyyy"))
 .withColumn("time string",date_format("timestamp","HH:mm:SS"))
 .withColumn("day of week",date_format("timestamp","E"))).display()

# COMMAND ----------

# DBTITLE 1,DATETIME Funtions
(events_df
 .withColumn("timestamp String",date_format("timestamp","MM-dd-yyyy HH:mm:ss"))
 .withColumn("Month",month("timestamp"))
 .withColumn("Day of Week",dayofweek("timestamp"))
 .withColumn("Year",year("timestamp"))
 .withColumn("Hour",hour("timestamp"))
 .withColumn("Minute",minute("timestamp"))
 .withColumn("Second",second("timestamp"))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Date Functions

# COMMAND ----------

# DBTITLE 1,date_add()
events_df = events_df.withColumn("After Two Days",date_add("timestamp",2))
events_df.display()

# COMMAND ----------

# DBTITLE 1,date_diff()
events_df = events_df.withColumn("Date Difference",date_diff("After Two Days","timestamp"))
events_df.display()

# COMMAND ----------

# DBTITLE 1,date_trunc()
events_df = events_df.withColumn("Date Trunc",date_trunc("mon","timestamp"))
events_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert to Date

# COMMAND ----------

# DBTITLE 1,to_date()
events_df = events_df.withColumn("Date Column",to_date("timestamp"))
events_df.display()

# COMMAND ----------

# DBTITLE 1,DateTime Scenario
from pyspark.sql.functions import *
active_users_df = (spark.table("events")
                   .withColumn("ts",(col("event_timestamp")/1e6).cast("timestamp"))
                   .withColumn("date",to_date("ts"))
                   .groupby("date")
                   .agg(approx_count_distinct("user_id").alias("active_users"))
                   .sort("date"))

(active_users_df
 .withColumn("day",date_format("date","E"))
 .groupby("day").agg(avg("active_users").alias("avg_users"))).display()

# COMMAND ----------

spark.table("events").display()

# COMMAND ----------


