# Databricks notebook source
# MAGIC %md
# MAGIC # Merge (Upsert) Operation in Delta Tables

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql. functions import *

schema = StructType([StructField("emp_id", IntegerType(), True), StructField("name", StringType(), True), StructField("city", StringType(), True), StructField("country", StringType(), True), StructField("contact_no", IntegerType(), True)])

# COMMAND ----------

data = [(1000,"Micheal","Columbus","USA",689546323)]
df = spark.createDataFrame(data=data, schema =schema)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/delta/path_employee_demo",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employee_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dim_employee(
# MAGIC   emp_id INT,
# MAGIC   name STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   contact_no INT
# MAGIC ) using delta
# MAGIC location '/FileStore/tables/delta_merge'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_employee as target
# MAGIC USING source_view as source
# MAGIC ON target.emp_id = source.emp_id
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE SET 
# MAGIC target.name = source.name,
# MAGIC target.city = source.city,
# MAGIC target.country = source.country,
# MAGIC target.conatact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

data = [(1000, "Michael", "Chicago", "USA", 689546323), (2000, "Nancy", "New York", "USA",76345902)]
df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_employee as target
# MAGIC USING source_view as source
# MAGIC ON target.emp_id = source.emp_id
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE SET 
# MAGIC target.name = source.name,
# MAGIC target.city = source.city,
# MAGIC target.country = source.country,
# MAGIC target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark Merge 

# COMMAND ----------

data= [(2000, "Sarah", "New York", "USA",76345902), (3000, "David", "Atlanta", "USA", 563456787)]
df=spark.createDataFrame (data = data, schema = schema)
display(df)

# COMMAND ----------

from delta.tables import *
delta_df = DeltaTable.forName(spark,"dim_employee")

# COMMAND ----------

display(df)

# COMMAND ----------

display(delta_df.toDF())

# COMMAND ----------

delta_df.alias ("target").merge(
source = df.alias("source"),
condition = "target.emp_id = source.emp_id"
).whenMatchedUpdate(set=
{
"name": "source.name",
"city": "source.city",
"country": "source.country",
"contact_no": "source.contact_no"
}
).whenNotMatchedInsert (values =
{
"emp_id": "source.emp_id",
"name": "source.name",
"city": "source.city",
"country": "source.country",
"contact_no": "source.contact_no"
}
).execute()

# COMMAND ----------

display(delta_df.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type - 2 Using Pyspark and Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Target Table using Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE scd2Demo (
# MAGIC pk1 INT, 
# MAGIC pk2 STRING, 
# MAGIC dim1 INT,
# MAGIC dim2 INT,
# MAGIC dim3 INT,
# MAGIC dim4 INT,
# MAGIC active_status STRING, 
# MAGIC start_date TIMESTAMP, 
# MAGIC end_date TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (111, 'Unit1', 200, 500, 800, 400, 'Y', current_timestamp (), '9999-12-31');
# MAGIC insert into scd2Demo values (222, 'Unit2', 900, Null, 700, 100, 'Y', current_timestamp (), '9999-12-31'); 
# MAGIC insert into scd2Demo values (333, 'Unit3', 300,900,250,650, 'Y', current_timestamp (), '9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forName(spark,"scd2Demo")
targetDF = targetTable.toDF()
display(targetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Source Data using Pyspark

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql. functions import *
schema = StructType([StructField("pk1", StringType(), True), \
  StructField("pk2", StringType(), True), \
  StructField("dim1", IntegerType(), True), \
  StructField("dim2", IntegerType(), True), \
  StructField("dim3", IntegerType(), True), \
  StructField("dim4", IntegerType(), True)])
data= [(111, 'Unit1', 200, 500, 800, 400),
(222,"Unit2",800,1300,800,500), (444,"Unit4", 100, None, 700, 300)]
sourceDF = spark.createDataFrame(data = data, schema = schema)
display(sourceDF)
display(targetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 1 : Left Join Source Table with Target Table

# COMMAND ----------

joinDF = sourceDF.join(targetDF,(sourceDF.pk1==targetDF.pk1) & (sourceDF.pk2==targetDF.pk2) & (targetDF.active_status == "Y"),"leftouter").select(\
sourceDF["*"],targetDF.pk1.alias("target_pk1").cast("String"), \
targetDF.pk2.alias("target_pk2"), \
targetDF.dim1.alias("target_dim1"), \
targetDF.dim2.alias("target_dim2"), \
targetDF.dim3.alias("target_dim3"), \
targetDF.dim4.alias("target_dim4"), \
targetDF.active_status.alias("target_active_status"), \
targetDF.start_date.alias("target_start_date"), \
targetDF.end_date.alias("target_end_date"))
display(joinDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 2 : Filter out Changed records 

# COMMAND ----------

filterDF = joinDF.filter(xxhash64(joinDF.dim1, joinDF.dim2, joinDF.dim3, joinDF.dim4)!= xxhash64(joinDF.target_dim1, joinDF.target_dim2, joinDF.target_dim3, joinDF.target_dim4))
display(filterDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 3 : Create a MERGE KEY by combining pk1 and pk2

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY",concat(col("pk1"),col("pk2")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 4 : Create a NULL MERGE KEY for matching records (Already existed records in Delta Table)

# COMMAND ----------

dummyDF = filterDF.filter("target_pk1 is not NULL").withColumn("MERGEKEY",lit(None))
display(dummyDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 5 : Union MergeDF and dummyDF 

# COMMAND ----------

scdDF = mergeDF.union(dummyDF)
display(scdDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 6 : Merge Operation 

# COMMAND ----------

targetTable.alias ("target").merge(
source = scdDF.alias("source"),
condition = "concat(target.pk1, target.pk2) = source.MERGEKEY and target.active_status = 'Y'" ).whenMatchedUpdate(set =
{
"active_status": "'N'",
"end_date": "current_date"
}
). whenNotMatchedInsert(values =
{
"pk1": "source.pk1", 
"pk2": "source.pk2", 
"dim1": "source.dim1", 
"dim2": "source.dim2",
"dim3": "source.dim3",
"dim4": "source.dim4",
"active_status": "'Y'",
"start_date": "current_date",
"end_date": """to_date('9999-12-31', 'yyyy-MM-dd')"""
}
).execute()

# COMMAND ----------

display(targetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC # Time Travel in Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark Approaches

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Method - 1 : Pyspark - Timestamp + Table 

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf","2024-04-12T20:31:38.000+0000").table("scd2Demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 2 : Pyspark - Timestamp + Path

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf","2024-04-12T20:31:38.000+0000").load("/FileStore/tables/scd2Demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 3 : Pyspark - Version + Table

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf","2").table("scd2Demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 4 : Pyspark -  Version + Path

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf","2").load("/FileStore/tables/scd2Demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Approaches

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 1 : SQL - Timestamp + Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2demo TIMESTAMP AS OF "2024-04-12T20:31:38.000+0000"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 2 : SQL - Timestamp + Path

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Delta.`/FileStore/tables/scd2Demo` TIMESTAMP AS OF "2024-04-12T20:31:38.000+0000"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 3 : SQL - Version + Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2demo VERSION AS OF "2"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 4 : SQL - VERSION + Path

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Delta.`/FileStore/tables/scd2Demo` VERSION AS OF "2"

# COMMAND ----------

# MAGIC %md
# MAGIC # Restore Delta Live Tables

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forPath(spark,"/FileStore/tables/scd2Demo")

# COMMAND ----------

display(targetTable.toDF())

# COMMAND ----------

display(targetTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore using Version Number

# COMMAND ----------

targetTable.restoreToVersion(3)

# COMMAND ----------

display(targetTable.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore using Timestamp

# COMMAND ----------

targetTable.restoreToTimestamp("2024-04-12T20:31:32.000+0000")

# COMMAND ----------

display(targetTable.toDF())

# COMMAND ----------

display(targetTable.history())

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/scd2Demo",True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta : Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE scd2Demo (
# MAGIC pk1 INT, 
# MAGIC pk2 STRING, 
# MAGIC diml INT,
# MAGIC dim2 INT,
# MAGIC dim3 INT,
# MAGIC dim4 INT,
# MAGIC active_status STRING, 
# MAGIC start_date TIMESTAMP,
# MAGIC end_date TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (111, 'Unit1', 200, 500, 800, 400, 'Y', current_timestamp (), '9999-12-31');
# MAGIC insert into scd2Demo values (222, 'Unit2', 900, Null, 700, 100, 'Y', current_timestamp (), '9999-12-31'); 
# MAGIC insert into scd2Demo values (333, 'Unit3', 300,900,250,650, 'Y', current_timestamp (), '9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (666, 'Unit6', 200, 500, 800, 400, 'Y', current_timestamp (), '9999-12-31');
# MAGIC insert into scd2Demo values (777, 'Unit7', 900, Null, 700, 100, 'Y', current_timestamp (), '9999-12-31'); 
# MAGIC insert into scd2Demo values (888, 'Unit8', 300,900,250,650, 'Y', current_timestamp (), '9999-12-31');

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/scd2Demo/_delta_log")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from scd2demo where pk1 = 777

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE scd2demo

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta : VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2Demo

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2Demo DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2demo RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2demo RETAIN 0 hours

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta : Z-Order Command

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE scd2demo
# MAGIC ZORDER pk1

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table : Merge Schema 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
.tableName("employee_demo") \
.addColumn("emp_id", "INT") \
.addColumn("emp_name", "STRING")\
.addColumn("gender", "STRING") \
.addColumn("salary", "INT") \
.addColumn("Dept", "STRING")\
.property("description", "table created for demo purpose")\
.location("/FileStore/tables/delta/path_employee_demo") \
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100,"Stephen","M",2000,"IT")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType 
employee_data = [(200, "Philipp", "M", 8000, "HR","test data")]
employee_schema = StructType([\
  StructField("emp_id", IntegerType(), False), \
  StructField("emp_name", StringType(), True), \
  StructField("gender", StringType(), True), \
  StructField("salary", IntegerType(), True), \
  StructField("dept", StringType(), True), \
  StructField("additonalcolumn1", StringType(), True)])
df = spark.createDataFrame(data = employee_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------


