# Databricks notebook source
# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC use default;
# MAGIC show tables in default;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema ap_dev_csrp_9100;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %sql
# MAGIC use ap_dev_csrp_9100;

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas in samples

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.nyctaxi.trips limit 20

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/learning-spark-v2/people/

# COMMAND ----------

display(spark.read.format("parquet")
        # .option("delimiter", ",")
        # .option("header",True)
        .load("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.parquet/"))

# COMMAND ----------

spark.read.format("parquet") \
.load("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.parquet/") \
.createOrReplaceTempView("people")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Female as (select * from people where Gender='F' LIMIT 500),
# MAGIC Male as (select * from people where Gender='M' LIMIT 500)
# MAGIC select * from Female union all select * from Male

# COMMAND ----------

spark.sql("""
WITH Female as (select * from people where Gender='F' LIMIT 500),
Male as (select * from people where Gender='M' LIMIT 500)
select * from Female union all select * from Male
""").write.format("delta").mode("overwrite").saveAsTable("workspace.ap_dev_csrp_9100.people")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from workspace.ap_dev_csrp_9100

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended workspace.ap_dev_csrp_9100.people

# COMMAND ----------

# MAGIC %fs ls dbfs:/my_data

# COMMAND ----------

spark.conf.get("spark.sql.warehouse.dir")

# COMMAND ----------


