# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Internal Architecture

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("delta_internal_demo") \
  .addColumn("emp_id", "INT") \
  .addColumn("emp_name", "STRING") \
  .addColumn("gender", "STRING") \
  .addColumn("salary", "INT") \
  .addColumn("Dept", "STRING") \
  .property("description", "table created for demo purpose") \
  .location("/FileStore/tables/delta/arch_demo") \
  .execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(100,"Stephen","M",2000,"IT")

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(200,"Philipp","M",8000,"HR");
# MAGIC insert into delta_internal_demo values(300,"Lara","F",6000,"SALES");

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch_demo

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(100,"Stephen","M",2000,"IT");
# MAGIC insert into delta_internal_demo values(200,"Philipp","M",8000,"HR");
# MAGIC insert into delta_internal_demo values(300,"Lara","F",6000,"SALES");
# MAGIC
# MAGIC insert into delta_internal_demo values(100,"Stephen","M",2000,"IT");
# MAGIC insert into delta_internal_demo values(200,"Philipp","M",8000,"HR");
# MAGIC insert into delta_internal_demo values(300,"Lara","F",6000,"SALES");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta_internal_demo where emp_id = 100

# COMMAND ----------

# DBTITLE 1,10-Log files(JSON) are created => 1 Checkpoint file created
# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log

# COMMAND ----------

display(spark.read.parquet("dbfs:/FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------

# DBTITLE 1,Delete operation Log information
# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.json

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Delta Tables in various methods

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method - 1 : Pyspark

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("employee_demo") \
  .addColumn("emp_id", "INT") \
  .addColumn("emp_name", "STRING") \
  .addColumn("gender", "STRING") \
  .addColumn("salary", "INT") \
  .addColumn("Dept", "STRING")\
  .property("description", "table created for demo purpose") \
  .location("/FileStore/tables/delta/createtable") \
  .execute()

# COMMAND ----------

from delta.tables import *

# DeltaTable.createIfNotExists(spark) \
#   .tableName("employee_demo") \
#   .addColumn("emp_id", "INT") \
#   .addColumn("emp_name", "STRING") \
#   .addColumn("gender", "STRING") \
#   .addColumn("salary", "INT") \
#   .addColumn("Dept", "STRING")\
#   .execute()

# DeltaTable.createOrReplace(spark) \
#   .tableName("employee_demo") \
#   .addColumn("emp_id", "INT") \
#   .addColumn("emp_name", "STRING") \
#   .addColumn("gender", "STRING") \
#   .addColumn("salary", "INT") \
#   .addColumn("Dept", "STRING")\
#   .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method - 2 : SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_demo(
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employee_demo(
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employee_demo(
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta/createtable'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method -3 : Using existing pyspark dataframe

# COMMAND ----------

employee_data = [(100, "Stephen", "M", 2000, "IT"),(200, "Philipp", "M", 8000, "HR"), (300, "Lara", "F", 6000,"SALES")]
employee_schema = ["emp_id", "emp_name", "gender", "salary","dept"]
df = spark.createDataFrame(data = employee_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format("delta").saveAsTable("default.employee_demo1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table Instance

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("employee_demo") \
  .addColumn("emp_id", "INT") \
  .addColumn("emp_name", "STRING") \
  .addColumn("gender", "STRING") \
  .addColumn("salary", "INT") \
  .addColumn("Dept", "STRING")\
  .property("description", "table created for demo purpose") \
  .location("/FileStore/tables/delta/createtable") \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100,"Stephen","M",2000,"IT");
# MAGIC insert into employee_demo values(200,"Philipp","M",8000,"HR");
# MAGIC insert into employee_demo values(300,"Lara","F",6000,"SALES");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using forPath

# COMMAND ----------

deltaInstance = DeltaTable.forPath(spark,"/FileStore/tables/delta/createtable")

# COMMAND ----------

display(deltaInstance.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employee_demo where emp_id=100

# COMMAND ----------

display(deltaInstance.toDF())

# COMMAND ----------

deltaInstance.delete("emp_id=200")

# COMMAND ----------

display(deltaInstance.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using forName

# COMMAND ----------

deltaInstance1 = DeltaTable.forName(spark,"employee_demo")

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee_demo

# COMMAND ----------

display(deltaInstance1.history())

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/delta/createtable",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC #Insert Data into Delta Table Using different approaches

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("employee_demo") \
  .addColumn("emp_id", "INT") \
  .addColumn("emp_name", "STRING") \
  .addColumn("gender", "STRING") \
  .addColumn("salary", "INT") \
  .addColumn("Dept", "STRING")\
  .property("description", "table created for demo purpose") \
  .location("/FileStore/tables/delta/createtable") \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Style Insertion

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100,"Stephen","M",2000,"IT");

# COMMAND ----------

display(spark.sql("select * from employee_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe Insertion

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
employee_data = [(200, "Philipp", "M", 8000, "HR")]
employee_schema = StructType([
StructField("emp_id", IntegerType(), False), \
StructField("emp_name", StringType(), True), \
StructField("gender", StringType(), True), \
StructField("salary", IntegerType(), True), \
StructField("dept", StringType(), True)])
df = spark.createDataFrame(data=employee_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe InsertInto Method

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
employee_data = [(300,"Lara","F",6000,"SALES")]
employee_schema = StructType([
StructField("emp_id", IntegerType(), False), \
StructField("emp_name", StringType(), True), \
StructField("gender", StringType(), True), \
StructField("salary", IntegerType(), True), \
StructField("dept", StringType(), True)])
df = spark.createDataFrame(data=employee_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.insertInto("employee_demo",overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert using Temp View

# COMMAND ----------

df.createOrReplaceTempView("delta_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL Insert

# COMMAND ----------

spark.sql("insert into employee_demo select * from delta_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/delta/createtable",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC #Approaches to delete data from Delta Table

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("employee_demo") \
  .addColumn("emp_id", "INT") \
  .addColumn("emp_name", "STRING") \
  .addColumn("gender", "STRING") \
  .addColumn("salary", "INT") \
  .addColumn("Dept", "STRING")\
  .property("description", "table created for demo purpose") \
  .location("/FileStore/tables/delta/path_employee_demo") \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100, "Stephen", "M", 2000, "IT"); 
# MAGIC insert into employee_demo values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into employee_demo values(300, "Lara", "F", 6000,"SALES");
# MAGIC insert into employee_demo values(400, "Mike", "M", 4000, "IT");
# MAGIC insert into employee_demo values(500, "Sarah", "F", 9000, "HR");
# MAGIC insert into employee_demo values(600, "Serena", "F", 5000, "SALES"); 
# MAGIC insert into employee_demo values (700, "Mark", "M", 7000,"SALES");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employee_demo where emp_id=100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using SQL with delta location

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.`/FileStore/tables/delta/path_employee_demo` where emp_id=200

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using SparkSQL Method

# COMMAND ----------

spark.sql("delete from employee_demo where emp_id=300")

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Pypsark delta table Instance

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import * 

deltaInstance = DeltaTable.forName(spark,"employee_demo")

deltaInstance.delete("emp_id=400")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple conditions using SQL Predicate

# COMMAND ----------

deltaInstance.delete("emp_id = 500 and gender = 'F'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark delete delta Instance - SparkSQL Predicate

# COMMAND ----------

deltaInstance.delete(col("emp_id")==600)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/delta/path_employee_demo",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC #Update records in delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table employee_demo(
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   Dept STRING
# MAGIC ) using delta
# MAGIC location '/FileStore/tables/delta/path_employee_demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100, "Stephen", "M", 2000, "IT"); 
# MAGIC insert into employee_demo values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into employee_demo values(300, "Lara", "F", 6000,"SALES");
# MAGIC insert into employee_demo values(400, "Mike", "M", 4000, "IT");
# MAGIC insert into employee_demo values(500, "Sarah", "F", 9000, "HR");
# MAGIC insert into employee_demo values(600, "Serena", "F", 5000, "SALES"); 
# MAGIC insert into employee_demo values (700, "Mark", "M", 7000,"SALES");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard SQL syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employee_demo SET salary = 10000 WHERE emp_name = 'Mark'

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Standard using TablePath

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE DELTA.`/FileStore/tables/delta/path_employee_demo` SET salary=12000 where emp_name = 'Mark'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark Standar using Table Instance

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import * 

deltaInstance = DeltaTable.forName(spark,"employee_demo")

deltaInstance.update(condition = "emp_name='Mark'", set = {"salary":"15000"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Instance with predicate

# COMMAND ----------

deltaInstance.update(condition = col('emp_name')=='Mark', set = {'Dept':lit('IT')})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo
