# Databricks notebook source
# MAGIC %md
# MAGIC # Read CSV Files - different options

# COMMAND ----------

#/FileStore/tables/files/Popular_Baby_Names.csv

dbutils.fs.ls("/FileStore/tables/files")

# COMMAND ----------

# MAGIC %scala
# MAGIC var df_scala = spark.read.format("csv")
# MAGIC .option("header",true)
# MAGIC //.option("sep",',')
# MAGIC .load("/FileStore/tables/files/Popular_Baby_Names.csv")
# MAGIC
# MAGIC display(df_scala.filter("Gender=='MALE' and `Year of Birth`=2011"))

# COMMAND ----------

# MAGIC %scala
# MAGIC var df_scala = spark.read.format("csv")
# MAGIC .option("header",true)
# MAGIC .option("inferschema",true)
# MAGIC .load("/FileStore/tables/files/Popular_Baby_Names.csv")
# MAGIC
# MAGIC display(df_scala.filter("Gender=='MALE' and `Year of Birth`=2011"))

# COMMAND ----------

df_python = spark.read.format("csv").option("header",True).option("sep",',').load("/FileStore/tables/files/Popular_Baby_Names.csv")

display(df_python.filter("Gender=='MALE' and `Year of Birth`=2011"))

# COMMAND ----------

import pandas as pd
pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3']
})
df = spark.createDataFrame(pandas_df)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Spark Query Excecution Plan

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# COMMAND ----------

df_customers = spark.read.parquet("/FileStore/tables/customer.parquet")

# COMMAND ----------

df_narrow_transform = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn("first_name", F.split("name", " ").getItem(0))
    .withColumn("last_name", F.split("name", " ").getItem(1))
    .withColumn("age", F.col("age") + F.lit(5))
    .select("cust_id", "first_name", "last_name", "age", "gender", "birthday")
)

df_narrow_transform.show(5, False)
df_narrow_transform.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark - Explode Function

# COMMAND ----------

# DBTITLE 1,Create DataFrame using Array Column
array_appliance =[
                  ('Raja', ['TV', 'Refrigerator', 'Oven', 'AC']), 
                  ('Raghav', ['AC', 'Washing machine', None]), 
                  ('Ram', ['Grinder', 'TV']),
                  ('Ramesh', ['Refrigerator', 'TV', None]), 
                  ('Rajesh', None)
                  ]
df_app = spark.createDataFrame(data=array_appliance, schema = ['name', 'Appliances'])
df_app.printSchema()
display (df_app)

# COMMAND ----------

# DBTITLE 1,Create DataFrame using Map Column
map_brand= [
('Raja', {'TV':'LG', 'Refrigerator': 'Samsung', 'Oven': 'Philipps', 'AC': 'Voltas'}), ('Raghav', {'AC': 'Samsung', 'Washing machine': 'LG'}),
('Ram', {'Grinder': 'Preethi', 'TV' : ''}),
('Ramesh', {'Refrigerator': 'LG', 'TV': 'Croma'}),
('Rajesh', None)]
df_brand= spark.createDataFrame(data=map_brand, schema = ['name', 'Brand'])
df_brand.printSchema()
display (df_brand)

# COMMAND ----------

# DBTITLE 1,Explode array field
from pyspark.sql.functions import explode
display(df_app.select(df_app.name,explode(df_app.Appliances)))

# COMMAND ----------

# DBTITLE 1,Explode map field
from pyspark.sql.functions import explode
display(df_brand.select(df_brand.name,explode(df_brand.Brand)))

# COMMAND ----------

# DBTITLE 1,Explode Outer to consider NULL Values
from pyspark.sql.functions import explode_outer
display(df_app.select(df_app.name,explode_outer(df_app.Appliances)))
display(df_brand.select(df_brand.name,explode_outer(df_brand.Brand)))

# COMMAND ----------

# DBTITLE 1,Posexplode - Positional Explode
from pyspark.sql.functions import posexplode
display(df_app.select(df_app.name,posexplode(df_app.Appliances)))
display(df_brand.select(df_brand.name,posexplode(df_brand.Brand)))

# COMMAND ----------

# DBTITLE 1,Positional explode with NULL values
from pyspark.sql.functions import posexplode_outer
display(df_app.select(df_app.name,posexplode_outer(df_app.Appliances)))
display(df_brand.select(df_brand.name,posexplode_outer(df_brand.Brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark - WHEN Otherwise (CASE)

# COMMAND ----------

# DBTITLE 1,Create a sample dataframe
data_student = [
                ("Raja", "Science", 80, "p",90),
                ("Rakesh", "Maths", 90, "P",70), 
                ("Rama", "English", 20, "F", 80), 
                ("Ramesh", "Science", 45, "F",75), 
                ("Rajesh", "Maths", 30, "F", 50), 
                ("Raghav", "Maths", None, "NA",70)
                ]
Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame (data = data_student, schema = Schema) 
display(df)

# COMMAND ----------

# DBTITLE 1,Update Existing Column
from pyspark.sql.functions import when

display(df.withColumn("Status", when(df.Mark >= 50,"Pass")
                                .when(df.Mark < 50, "Fail").otherwise("Absentee")))

# COMMAND ----------

# DBTITLE 1,Another Syntax method
from pyspark.sql.functions import expr

display(df.withColumn("new_status", expr("CASE WHEN Mark >= 50 THEN 'PASS'" + "WHEN Mark < 50 THEN 'FAIL'" + "ELSE 'ABSENT' END")))

# COMMAND ----------

# DBTITLE 1,Multiple conditions using AND and OR operators
from pyspark.sql.functions import when

display(df.withColumn("Grade", when((df.Mark >= 80) & (df.Attendance>=80),"Distinction")
                               .when((df.Mark >= 50) & (df.Attendance>=50), "Good")
                               .otherwise("Average")))


display(df.withColumn("Grade", when((df.Mark >= 80) | (df.Attendance>=80),"Distinction")
                               .when((df.Mark >= 50) & (df.Attendance>=50), "Good")
                               .otherwise("Average")))

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark - Union and UnionAll

# COMMAND ----------

# DBTITLE 1,Spark Version
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()
print(spark.sparkContext.version)

# COMMAND ----------

# DBTITLE 1,Create DataFrame DF1
employee_data = [(100,"Stephen", "1999", "100", "M", 2000),
                 (200, "Philipp", "2002", "200","M", 8000),
                 (300, "John", "2010", "100", "", 6000),
                ]
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary"]
DF1 = spark.createDataFrame (data=employee_data, schema = employee_schema)
display (DF1)

# COMMAND ----------

# DBTITLE 1,Create Data Frame DF2
employee_data = [
                  (300, "John", "2010", "100", "", 6000), 
                  (400, "Nancy", "2008", "400", "F", 1000), 
                  (500, "Rosy", "2014", "500", "M", 5000)
                ]
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary"]
DF2 = spark.createDataFrame (data=employee_data, schema = employee_schema)
display (DF2)

# COMMAND ----------

display(DF1.union(DF2))
display(DF1.unionAll(DF2))

# COMMAND ----------

display(DF1.union(DF2).dropDuplicates())
display(DF1.unionAll(DF2).dropDuplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark - Pivot & UnPivot

# COMMAND ----------

# DBTITLE 1,Create a sample DataFrame
data =[
        ("ABC", "Q1", 2000), 
        ("ABC", "Q2", 3000),
        ("ABC", "Q3",6000),
        ("ABC", "Q4", 1000),
        ("XYZ", "Q1", 5000),
        ("XYZ", "Q2", 4000),
        ("XYZ", "Q3", 1000),
        ("XYZ", "Q4", 2000),
        ("KLM", "Q1", 2000),
        ("KLM", "Q2", 3000),
        ("KLM", "Q3", 1000),
        ("KLM", "Q4", 5000) 
      ]
column= ["Company", "Quarter", "Revenue"]
DF = spark.createDataFrame(data = data, schema = column) 
display(DF)

# COMMAND ----------

# DBTITLE 1,Pivot a DataFrame
DF_Pivot = DF.groupBy("Company").pivot("Quarter").sum("Revenue")
display(DF_Pivot)

# COMMAND ----------

# DBTITLE 1,Unpivot a DataFrame
DF_Unpivot = DF_Pivot.selectExpr("Company","stack(4,'Q1',Q1,'Q2',Q2,'Q3',Q3,'Q4',Q4) as (Quarter,Revenue)")
display(DF_Unpivot)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pyspark - Corrupt Records Mode

# COMMAND ----------

# DBTITLE 1,Create a Data Frame
df = spark.read.format("csv").option("header",True).option("inferschema",True).load("/FileStore/tables/files/CorruptFile.csv")
display(df)

# COMMAND ----------

# DBTITLE 1,Define Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([\
  StructField("Month", StringType(), True), \
  StructField("Emp_count", IntegerType(), True), \
  StructField("Production_unit", IntegerType(), True),\
  StructField("Expense", IntegerType(), True),\
  StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Permissive Mode
df = spark.read.format("csv")\
               .option("mode","PERMISSIVE")\
               .option("header","true")\
               .schema(schema)\
               .load("/FileStore/tables/files/CorruptFile.csv")

display(df)

# COMMAND ----------

# DBTITLE 1,DropMalformed Mode
df = spark.read.format("csv")\
               .option("mode","DROPMALFORMED")\
               .option("header","true")\
               .schema(schema)\
               .load("/FileStore/tables/files/CorruptFile.csv")

display(df)

# COMMAND ----------

# DBTITLE 1,FailFast Mode
df = spark.read.format("csv")\
               .option("mode","FAILFAST")\
               .option("header","true")\
               .schema(schema)\
               .load("/FileStore/tables/files/CorruptFile.csv")

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # ADLS Connection

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method - 1 Using ADLS Key Directly 

# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.adlsdepractice.dfs.core.windows.net",
"wCBornYEB/fHjlM4i+dICZtTKHKxAitxTemy27POUdrCe05U1JbUqijl6/3bbeMefQKH2+vYrIa6+AStZLxm/g=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://container-de-practice@adlsdepractice.dfs.core.windows.net/")

# COMMAND ----------

file_loc = "abfss://container-de-practice@adlsdepractice.dfs.core.windows.net/"

df = spark.read.option("inferSchema",True).option("header",True).option("delimiter",",").csv(file_loc)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method - 2 Creatng MountPoint using ADLS Access key

# COMMAND ----------

dbutils.fs.mount(
source= "wasbs://container-de-practice@adlsdepractice.blob.core.windows.net/",
mount_point = "/mnt/adls_test",
extra_configs = {"fs.azure.account.key.adlsdepractice.blob.core.windows.net":"wCBornYEB/fHjlM4i+dICZtTKHKxAitxTemy27POUdrCe05U1JbUqijl6/3bbeMefQKH2+vYrIa6+AStZLxm/g=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/adls_test")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Read tables from Azure SQL Database

# COMMAND ----------

jdbcHostname = "ss-de-practice.database.windows.net" #Server
jdbcPort = 1433
jdbcDatabase = "database_de_practice"
jdbcUsername = "depractice"
jdbcPassword = "Chaypagadala#31"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

print(jdbcUrl)

# COMMAND ----------

df = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","SalesLT.Product").load()
display(df)

# COMMAND ----------

connectionString = "jdbc:sqlserver://ss-de-practice.database.windows.net:1433;database=database_de_practice;user=depractice@ss-de-practice;password={Chaypagadala#31};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

df1 = spark.read.jdbc(connectionString, "SalesLT.Product")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL Pipeline : Azure SQL to ADLS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 1 : Read tables from Azure SQL Database

# COMMAND ----------

# DBTITLE 1,Define JDBC Connection Parameters
jdbcHostname = "ss-de-practice.database.windows.net" #Server
jdbcPort = 1433
jdbcDatabase = "database_de_practice"
jdbcUsername = "depractice"
jdbcPassword = "Chaypagadala#31"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

print(jdbcUrl)

# COMMAND ----------

# DBTITLE 1,Read Product Table
df_product = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","SalesLT.Product").load()
display(df_product)

# COMMAND ----------

# DBTITLE 1,Read Sales Table
df_sales = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","SalesLT.SalesOrderDetail").load()
display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 2 : Transform the data as per business rules

# COMMAND ----------

# DBTITLE 1,Cleansing Dimension Dataframe - Replace NULL Values
df_product_cleansed = df_product.na.fill({"Size" : "M", "Weight" : 100})
display(df_product_cleansed)

# COMMAND ----------

# DBTITLE 1,Cleansing Fact Dataframe - Drop Duplicate Records
df_sales_cleansed = df_sales.dropDuplicates()
display(df_sales_cleansed)

# COMMAND ----------

# DBTITLE 1,Join Dimension and Fact tables
df_join = df_sales_cleansed.join(df_product_cleansed,df_sales_cleansed.ProductID == df_product_cleansed.ProductID,"leftouter").select(df_sales_cleansed.ProductID,
                                   df_sales_cleansed.UnitPrice,
                                   df_sales_cleansed.LineTotal,
                                   df_product_cleansed.Name,
                                   df_product_cleansed.Color,
                                   df_product_cleansed.Size,
                                   df_product_cleansed.Weight)
                           
display(df_join)

# COMMAND ----------

df_agg = df_join.groupBy(["ProductID", "Name", "Color", "Size", "Weight"]).sum("LineTotal").withColumnRenamed("sum(LineTotal)", "sum_total_sales") 
display(df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 3 : Load Data into Azure DataLake Storage

# COMMAND ----------

# DBTITLE 1,Create Mount Point for ADLS Integration
dbutils.fs.mount(
source= "wasbs://container-de-practice@adlsdepractice.blob.core.windows.net/",
mount_point = "/mnt/adls_test",
extra_configs = {"fs.azure.account.key.adlsdepractice.blob.core.windows.net":"wCBornYEB/fHjlM4i+dICZtTKHKxAitxTemy27POUdrCe05U1JbUqijl6/3bbeMefQKH2+vYrIa6+AStZLxm/g=="})

# COMMAND ----------

# DBTITLE 1,Load files under the Mount Point
dbutils.fs.ls("/mnt/adls_test")

# COMMAND ----------

# DBTITLE 1,Write the data in Parquet Format
df_agg.write.format("parquet").save("/mnt/adls_test/adv_work_parquet/")

# COMMAND ----------

# DBTITLE 1,Write the data in csv Format
df_agg.write.format("csv").option("header",True).save("/mnt/adls_test/adv_work_parquet.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC #Window Functions - Lead and Lag Transformations

# COMMAND ----------

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("James", "Sales", 3000), \
              ("Saif", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000))
columns= ["employee_name", "department", "salary"] 
df = spark.createDataFrame(data=simpleData, schema = columns)
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("department").orderBy("salary")

# COMMAND ----------

from pyspark.sql.functions import lag
df = df.withColumn("lag",lag("salary",2).over(windowSpec))
display(df)

# COMMAND ----------

from pyspark.sql.functions import lead
df = df.withColumn("lead",lead("salary",1).over(windowSpec))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance Optimization : Bucketing

# COMMAND ----------

spark.conf.get("spark.sql.sources.bucketing.enabled")

# COMMAND ----------

from pyspark.sql.functions import col,rand

df = spark.range(1,10000,10).select(col("id").alias("PK"),rand(10).alias("Attribute"))

df.display()

# COMMAND ----------

df.write.format("parquet").saveAsTable("NonbucketedTable")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/nonbucketedtable

# COMMAND ----------

df.write.format("parquet").bucketBy(10,"PK").saveAsTable("bucketedTable")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/bucketedtable

# COMMAND ----------

df1 = spark.table("bucketedTable")
df2 = spark.table("bucketedTable")

df3 = spark.table("nonbucketedTable")
df4 = spark.table("nonbucketedTable")

# COMMAND ----------

df3.join(df4, "PK","inner").explain()

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled",False)

# COMMAND ----------

display(df3.join(df4, "PK","inner"))

# COMMAND ----------

df3.join(df4, "PK","inner").explain()

# COMMAND ----------

df3.join(df1, "PK").display()

# COMMAND ----------

df3.join(df1, "PK").explain()

# COMMAND ----------

df1.join(df2, "PK").display()

# COMMAND ----------

df1.join(df2, "PK").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # Max Over Window Function

# COMMAND ----------

simpleData= ((100, "Mobile", 5000, 10), \
  (100, "Mobile", 7000,7), \
  (200, "Laptop", 20000,4), \
  (200, "Laptop", 25000,8), \
  (200, "Laptop", 22000,12))

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
defSchema = StructType([\
StructField("Product_id", IntegerType(), False), \
StructField("Product_name", StringType(), True), \
StructField("Price", IntegerType(), True), \
StructField("DiscountPercent", IntegerType(), True)])

df = spark.createDataFrame(data = simpleData, schema = defSchema)
display(df)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import max,col

windowSpec = Window.partitionBy('Product_id')

dfMax = df.withColumn('maxPrice', max('Price').over(windowSpec)).withColumn('maxDiscountPercent', max('DiscountPercent').over(windowSpec))

display(dfMax)

# COMMAND ----------

dfSel = dfMax.select(col("Product_id"),col("Product_name"),col("maxPrice").alias("Price"),col("maxDiscountPercent").alias("DiscountPercent"))

display(dfSel)

# COMMAND ----------

dfOut = dfSel.dropDuplicates()

# COMMAND ----------

display(dfOut)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pyspark Interview preparation

# COMMAND ----------

data1=[(1,"A",1000,"IT"),(2,"B",1500,"IT"),(3,"C",2500,"IT"),(4,"D",3000,"HR"),(5,"E",2000,"HR"),(6,"F",1000,"HR")
       ,(7,"G",4000,"Sales"),(8,"H",4000,"Sales"),(9,"I",1000,"Sales"),(10,"J",2000,"Sales")]
schema1=["EmpId","EmpName","Salary","DeptName"]
df=spark.createDataFrame(data1,schema1)
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
df.select("*",dense_rank().over(Window.partitionBy("DeptName").orderBy(desc("Salary"))).alias("Rank")).filter("Rank=1").show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
df1 = df.select("*",dense_rank().over(Window.partitionBy(df.DeptName).orderBy(df.Salary.desc())).alias("Rank"))
df1.filter(df1.Rank==1).show()

# COMMAND ----------

#Employees Salary info
data1=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),(200,"Joanne",100,1,"13-04-23",4500),(200,"Joanne",100,1,"14-04-23",4020)]
schema1=["EmpId","EmpName","Mgrid","deptid","salarydt","salary"]
df_salary=spark.createDataFrame(data1,schema1)
display(df_salary)
#department dataframe
data2=[(1,"IT"),
       (2,"HR")]
schema2=["deptid","deptname"]
df_dept=spark.createDataFrame(data2,schema2)
display(df_dept)

# COMMAND ----------


