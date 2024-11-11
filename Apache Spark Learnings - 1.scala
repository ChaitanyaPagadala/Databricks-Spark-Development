// Databricks notebook source
// MAGIC %md
// MAGIC # CRUD Operations

// COMMAND ----------

// MAGIC %md
// MAGIC ### CREATE Operation

// COMMAND ----------

val a = (1 to 10).toList
val b = ('a' to 'z').toList
val c = (1 until 10).toList
val d = (1 to 10 by 2).toList
val e = (1 to 10).by(2).toList

// Range function 
val f = List.range(1,10)
val g = List.range(1,10,2)

// Fill and Tabulate
val h = List.fill(3)(1)
val i = List.fill(3)("chay")

val j = List.tabulate(3)(n => n)
val k = List.tabulate(5)(n => n*10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### READ Operation

// COMMAND ----------

println("Iteration using Range function")
val l = List(1,2,3,4,5)
for(i<-List.range(0,l.length))
{
  println(l(i))
}

println("Iteration using Tabulate function")
for(i<-List.tabulate(l.length)(n=>n))
{
  println(l(i))
}

println("yield Function Utilization")
val oneToFive = List(1, 2, 3, 4, 5)
val a = for (i <- oneToFive) yield i
val b = for (i <- oneToFive) yield i * 2
val c = for (i <- oneToFive) yield i % 2

// COMMAND ----------

// MAGIC %md
// MAGIC ### UPDATE Operation
// MAGIC ##### List Append/Prepend Opeartions
// MAGIC
// MAGIC ##### List Update Operations
// MAGIC

// COMMAND ----------

//Prepend elements to the list
val a = List(2,3)
val b = 1::a
val c = 0::b

// map, distinct, updated functions
val x = List(1,2,1,2)
val d = x.distinct
val e = x.map(_*3)
val f = x.updated(1,100)

val g = List(List(1,2),List(3,4))
val h = g.flatten

val fruits = List("apple", "pear")
val i = fruits.map(_.toUpperCase)
val j = fruits.flatMap(_.toUpperCase)

val k = List(2,4).union(List(1,3)) 

// COMMAND ----------

// MAGIC %md
// MAGIC ### DELETE Operation
// MAGIC ##### List Delete Operations
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC #### Tranformational Methods

// COMMAND ----------

val a = List(10, 20, 30, 40, 10)
val b = a.distinct
val c = a.drop(2)
val d = a.dropRight(3)
val e = a.dropWhile(_ < 25)
val f = a.filter(_ < 25)
val g = a.filterNot(_ > 100)
val h = a.find(_ < 25)
val i = a.head
val j = a.headOption
val k = a.init
val l = a.intersect(List(19,20,21))
val m = a.last
val n = a.lastOption
val o = a.slice(0,a.length)
val p = a.tail
val q = a.take(3)
val r = a.takeRight(3)
val s = a.takeWhile(_ < 30)

//Transformer Methods
val t = a.init.reverse
val u = a.init.sorted
val v = a.init.sortWith(_<_)
val w = a.init.sortWith(_>_)

//zip
val women = List("Wilma", "Betty")
val men = List("Fred", "Barney")  
val couples = women.zip(men)      

val x = List.range('a', 'e')     
x.zipWithIndex                   

// COMMAND ----------

// MAGIC %md
// MAGIC #### Informational and Mathematical Methods

// COMMAND ----------

val evens = List(2, 4, 6)            
val odds = List(1, 3, 5)             
val fbb = "foo bar baz"              
val firstTen = (1 to 10).toList      
val fiveToFifteen = (5 to 15).toList 
val empty = List[Int]()              
val letters = ('a' to 'f').toList    

// COMMAND ----------

val a = evens.contains(2)                          
val b = firstTen.containsSlice(List(3,4,5))        
val c = firstTen.count(_ % 2 == 0)                 
val d = firstTen.endsWith(List(9,10))              
val e = firstTen.exists(_ > 10)                    
val f = firstTen.find(_ > 2)                       
val g = firstTen.forall(_ < 20)                    
val h = firstTen.hasDefiniteSize                   
val i = empty.hasDefiniteSize                      
val j = letters.indexOf('b')                       
val k = letters.indexOf('d', 2)                    
val l = letters.indexOf('d', 3)                    
val m = letters.indexOf('d', 4)                    
val n = letters.indexOfSlice(List('c','d'))        
val o = letters.indexOfSlice(List('c','d'),2)      
val p = letters.indexOfSlice(List('c','d'),3)      
val q = firstTen.indexWhere(_ == 3)                
val r = firstTen.indexWhere(_ == 3, 2)             
val s = firstTen.indexWhere(_ == 3, 5)             
val t = letters.isDefinedAt(1)                     
val u = letters.isDefinedAt(20)                    
val v = letters.isEmpty                            
val w = empty.isEmpty                              

// COMMAND ----------

val a =firstTen.max                              
val b =letters.max                               
val c =firstTen.min                              
val d =letters.min                               
val e =letters.nonEmpty                          
val f =empty.nonEmpty                            
val g =firstTen.product                          
val h =letters.size                              

// COMMAND ----------

// MAGIC %md
// MAGIC #### Grouping methods

// COMMAND ----------

val firstTen = (1 to 10).toList      

val a = firstTen.groupBy(_ > 5)              
val b = firstTen.grouped(2)                  
val c = firstTen.grouped(2).toList           
val d = firstTen.grouped(5).toList           

val e = "foo bar baz".partition(_ < 'c')     
val f = firstTen.partition(_ > 5)            

val g =firstTen.sliding(2)                  
val h =firstTen.sliding(2).toList           
val i =firstTen.sliding(2,2).toList         
val j =firstTen.sliding(2,3).toList         
val k = firstTen.sliding(2,4).toList         

val l = List(15, 10, 5, 8, 20, 12)
val m = l.groupBy(_ > 10)                    
val n = l.partition(_ > 10)                  
val o = l.span(_ < 20)                       
val p = l.splitAt(2)                         

// COMMAND ----------

// MAGIC %md
// MAGIC #RDD
// MAGIC ####Resilient Distributed Datasets (RDD) is the fundamental data structure of Spark. RDDs are immutable and fault-tolerant in nature. RDD is just the way of representing Dataset distributed across multiple nodes in a cluster, which can be operated in parallel. RDDs are called resilient because they have the ability to always re-compute an RDD when a node failure.
// MAGIC
// MAGIC ####Let’s see how to create an RDD in Apache Spark with examples:
// MAGIC #####1. Spark create RDD from Seq or List  (using Parallelize)
// MAGIC #####2. Creating an RDD from a text file
// MAGIC #####3. Creating from another RDD
// MAGIC #####4. Creating from existing DataFrames and DataSet
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## EmptyRDD Creation 

// COMMAND ----------

val emptyRDD1 = sc.parallelize(Seq.empty[String])
val emptyRDD2 = sc.emptyRDD[String]

// COMMAND ----------

// MAGIC %md
// MAGIC ## parallelize()

// COMMAND ----------

val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

// COMMAND ----------

// MAGIC %md
// MAGIC ## makeRDD()

// COMMAND ----------

val rdd = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9,10)).coalesce(1)
val rdd1 = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10)).coalesce(1)

// COMMAND ----------

// MAGIC %md
// MAGIC ## RDD Functions

// COMMAND ----------

val rdd = sc.parallelize(Array(10,20,30,40,50,60,70,80,90,100))
println("Count Function : " + rdd.count())
println("First Function : " + rdd.first())
println("CountByValue Function : " + rdd.countByValue())
println("Filter and MkString Function : " + rdd.filter(x=>x%2==0).collect().mkString(","))
println("min Function : " + rdd.min + " max Function : " + rdd.max + " sum Function : " + rdd.sum)
println("take Function : " + rdd.collect().take(5).mkString(","))
// println("toDataFrame : " + rdd.toDF("Chay").show())

// COMMAND ----------

// MAGIC %md
// MAGIC ## RDD Partitions

// COMMAND ----------

//rdd partititons size
val rdd = sc.parallelize(Array(10,20,30,40,50,60,70,80,90,100))
println(rdd.partitions.size)
println("====="*10)

//getNumPartitions
val rdd1 = sc.parallelize(Array(10,20,30,40,50,60,70,80,90,100),2)
println(rdd1.getNumPartitions)
println(rdd1.partitions.size)
println("====="*10)

//Size of Each Partition
println(rdd.mapPartitions(x=>Array(x.size).iterator).collect().mkString(","))
println("====="*10)

//Size of Each Partition
println(rdd1.mapPartitions(x=>Array(x.size).iterator).collect().mkString(","))
println("====="*10)


//glom()
println(rdd1.glom().collect()
.foreach(a => 
{
a.foreach(println);
println("=====")
}
))

// COMMAND ----------

// MAGIC %python
// MAGIC a = sc.parallelize(range(10), 5)
// MAGIC a.glom().collect()

// COMMAND ----------

// MAGIC %md
// MAGIC #DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ### toDF()
// MAGIC ######toDF() method provides a very concise way to create a Dataframe. This method can be applied to a sequence of objects.
// MAGIC ######NO CONTROL OVER SCHEMA CUSTOMIZATION

// COMMAND ----------

val empDataFrame = Seq(("Alice", 24), ("Bob", 26))
display(empDataFrame.toDF("name","age"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### createDataFrame()
// MAGIC ######The createDataFrame() method addresses the limitations of the toDF() method. With createDataFrame() method we have control over complete schema customization.
// MAGIC ######FULL CONTROL OVER SCHEMA CUSTOMIZATION

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
val empData = sc.parallelize(Seq(Row("Alice", 24), Row("Bob", 26)))
val empSchema = StructType(StructField("name", StringType, true)::StructField("age", IntegerType, true)::Nil)
display(spark.createDataFrame(empData,empSchema))

// COMMAND ----------

// MAGIC %md
// MAGIC ###Read from External Source - CSV File

// COMMAND ----------

// MAGIC %md
// MAGIC ####Reading a csv File with InferSchema.
// MAGIC ######Spark Jobs - 3

// COMMAND ----------

val df = spark.read.option("header",true).option("inferschema",true
).csv("/FileStore/tables/Popular_Baby_Names.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Reading a csv File without Schema Control.
// MAGIC ######Spark Jobs - 2

// COMMAND ----------

val df = spark.read.option("header",true).csv("/FileStore/tables/Popular_Baby_Names.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Reading a csv File with Schema
// MAGIC ######Spark Jobs - 1

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = StructType(
  StructField("Year of Birth",IntegerType,false)::
  StructField("Gender",StringType,false)::
  StructField("Ethnicity",StringType,false)::
  StructField("Child's First Name",StringType,false)::
  StructField("Count",IntegerType,false)::
  StructField("Rank",IntegerType,false)::Nil)

val df = spark.read.option("header",true).schema(schema).csv("/FileStore/tables/Popular_Baby_Names.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Columns and Expressions

// COMMAND ----------

import org.apache.spark.sql.functions._
import java.sql.Date
val schema = StructType(Array(
  StructField("Date",DateType,true),
  StructField("Transaction_ID",IntegerType,true),
  StructField("Customer_ID",IntegerType,true),
  StructField("Product_ID",IntegerType,true),
  StructField("Quantity",IntegerType,true),
  StructField("Price",DoubleType,true),
  StructField("Total_Sale",DoubleType,true),
  StructField("Product_Name",StringType,true),
  StructField("Place",StringType,true)))
val colExpr = spark.read.option("header",true).schema(schema).csv("/FileStore/tables/sales_data_with_product_and_place.csv")
display(colExpr)

// COMMAND ----------

colExpr.columns

// COMMAND ----------

colExpr.col("Transaction_ID")

// COMMAND ----------

// MAGIC %md
// MAGIC ####expr 
// MAGIC ######expr() takes arguments that Spark will parse as an expression, computing the result

// COMMAND ----------

colExpr.select(expr("Price * 2")).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ####col
// MAGIC ######selecting a column with col function

// COMMAND ----------

colExpr.select(col("Price")* 2).show(5)

// COMMAND ----------

colExpr.select(expr("Price")).show(2)
colExpr.select(col("Price")).show(2)
colExpr.select("Price").show(2)

// COMMAND ----------

// MAGIC %md
// MAGIC ####concat() and concat_ws()
// MAGIC ######Concatenate three columns and form a new column

// COMMAND ----------

// DBTITLE 1,concat()
colExpr.withColumn("concatenatedColumn",concat(expr("Product_Name"),expr("Place"))).show(10)

// COMMAND ----------

// DBTITLE 1,concat_ws()
colExpr.withColumn("concatenatedColumn",concat_ws("::",expr("Product_Name"),expr("Place"))).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Sorting

// COMMAND ----------

colExpr.sort(col("product_ID").desc).show(10)

// COMMAND ----------

colExpr.sort($"product_ID".desc).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Rows

// COMMAND ----------

val row = Row(1,2,3,4,5)
row(2)

// COMMAND ----------

val row = Seq((1,2),(10,20))
sc.parallelize(row).toDF("a","b").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##DataFrame Operations

// COMMAND ----------

// DBTITLE 1,DataFrameReader
val df = spark.read.option("header",true).option("inferSchema",true).csv("/FileStore/tables/sales_data_with_product_and_place.csv")
display(df)

// COMMAND ----------

//Spark can infer schema from a sample at a lesser cost. 
//For example, you can use the samplingRatio option:
//Spark will scan only the amount of data mentioned in samplingRatio rather than complete scan of all the rows.

val df = spark.read.option("header",true).option("inferSchema",true).option("samplingRatio",0.01).csv("/FileStore/tables/sales_data_with_product_and_place.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ####DataFrameWriter

// COMMAND ----------

// DBTITLE 1,Save as Parquet File
//Parquet, a popular columnar format, is the default format; 
//It uses snappy compression to compress the data. 
//If the DataFrame is written as Parquet, the schema is preserved as part of the Parquet metadata. 
//In this case, subsequent reads back into a DataFrame do not require you to manually supply a schema.

val parquetPath="/FileStore/tables/SALES_dATA"
df.write.mode("overwrite").format("parquet").save(parquetPath)

// COMMAND ----------

// DBTITLE 1,Save as a SQL Table
val table_name="SALES_dATA"
df.write.mode("overwrite").format("parquet").saveAsTable(table_name)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Transformations and actions

// COMMAND ----------

// MAGIC %md
// MAGIC ####Projections and Filters.

// COMMAND ----------

// DBTITLE 1,select() -- where() -- filter()
//select() and filter() and where()
//Two different ways of writing 
df.select("*").filter(col("product_ID") === 100).show()
df.select("*").where(col("product_ID") === 100).show()

df.select("*").filter("Product_ID == 100").show()
df.select("*").filter($"Product_ID" === 100).show()

df.select("*").where("Product_ID == 100").show()
df.select("*").where($"Product_ID" === 100).show()

// COMMAND ----------

// DBTITLE 1,agg(countDistinct()) -- distinct()
df.select("Product_ID").where(col("Product_ID").isNotNull).agg(countDistinct("Product_ID")).show()
df.select("Product_ID").where(col("Product_ID").isNotNull).distinct().show()


df.select("Product_ID").where($"Product_ID".isNotNull).agg(countDistinct("Product_ID")).show()
df.select("Product_ID").where($"Product_ID".isNotNull).distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####Renaming, adding, and dropping columns

// COMMAND ----------

// DBTITLE 1,withColumnRenamed()
val newDf = df.withColumnRenamed("Place","Store_Name")
newDf.filter($"Store_Name" === "Store_1").show()

// COMMAND ----------

// DBTITLE 1,to_timestamp()
val tsDF = newDf.withColumn("Date",to_timestamp(col("Date"),"MM/dd/yyyy"))
tsDF.show(10)

// COMMAND ----------

// DBTITLE 1,month(), year(), and dayofmonth()
tsDF.select(year($"Date")).distinct().show()
tsDF.select(month($"Date")).distinct().show()
tsDF.select(dayofmonth($"Date")).distinct().sort($"Date").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####Aggregations

// COMMAND ----------

display(tsDF.filter($"Product_ID" === 100))

// COMMAND ----------

// DBTITLE 1,groupBy() -- orderBy() -- count()
tsDF.groupBy("Product_ID","Product_Name").agg(sum("Quantity")).orderBy("Product_ID").show(10)

// COMMAND ----------

tsDF.groupBy("Product_ID").agg(countDistinct("Product_Name")).orderBy("Product_ID").show(10)

// COMMAND ----------

// DBTITLE 1,sum() -- min() --max() -- avg()
tsDF.filter("Product_ID==100").select(
  countDistinct("Store_Name").alias("Stores Count"),
  sum("Quantity").alias("Quantity Sum"),
  min("Transaction_ID").alias("Min Transaction ID"),
  max("Total_Sale").alias("Max Total Sale"),
  avg("Price").alias("Avg Price")
  ).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #DataSet
// MAGIC

// COMMAND ----------

// DBTITLE 1,Row Object
val row = Row(350, true, "Learning Spark 2E", null)
println(row.get(0))
println(row.getInt(0))
println(row.getBoolean(1))
println(row.getString(2))

// COMMAND ----------

// DBTITLE 1,CASE Class -- Dataset Creation
case class salary_schema(Name:String,Age:Int,Salary:Int)
val ds = spark.read.option("inferSchema",true).csv("/FileStore/tables/user_details.csv").toDF("Name","Age","Salary").as[salary_schema]

// COMMAND ----------

// MAGIC %md
// MAGIC ##Dataset Operations

// COMMAND ----------

import java.time.LocalDate
case class Sales_Schema(Date:LocalDate,Transaction_ID:Int,Customer_ID:Int,Product_ID:Int,Quantity:Int,Price:Double,Total_Sale:Double,Product_Name:String,Place:String)
val ds = df.as[Sales_Schema]

// COMMAND ----------

ds.show(5)

// COMMAND ----------

// DBTITLE 1,filter() -- map()
case class onlyProductDetails(Product_ID:Double,Price:Double,Product_Name:String,Store_Name:String)

val productID110 = ds.filter(d => {d.Product_ID>109 && d.Quantity>5})
.map(d => (d.Product_ID,d.Price,d.Product_Name,d.Place))
.toDF("Product_ID","Price","Product_Name","Store_Name")
.as[onlyProductDetails]

display(productID110)

// COMMAND ----------

// DBTITLE 1,select() -- where()
case class onlyProductDetails(Product_ID:Double,Price:Double,Product_Name:String,Store_Name:String)

val productID110_different_way = ds.select($"Product_ID",$"Price",$"Product_Name",$"Place".as("Store_Name"))
.where("Product_ID > 109 and Quantity > 5")
.as[onlyProductDetails]

display(productID110_different_way)

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark SQL - Dataframe

// COMMAND ----------

// DBTITLE 1,List of databases
display(spark.catalog.listDatabases)


// COMMAND ----------

// DBTITLE 1,Database Creation
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("use learn_spark_db")

// COMMAND ----------

// DBTITLE 1,Database Location and Hive Table Location
import sqlContext.implicits._ // for `toDF` and $""
import org.apache.spark.sql.functions._ // for `when`

def getHiveTableLocation(hiveDbName:String,tableName:String): String = {
    val tablePath = getHiveDbLocation( hiveDbName) + tableName.toLowerCase()
    println("SparkUtil, Get Hive Table Location ==>"+tablePath)
    tablePath
  }
  
  def getHiveDbLocation(hiveDbName:String): String = {
    val hiveWarehousePath = spark.conf.getOption("spark.sql.warehouse.dir").get
    val dbPath = hiveWarehousePath.stripSuffix("/") +"/"+ hiveDbName + ".db/"
    println("SparkUtil, Get Hive Database Location ==>"+dbPath)
    dbPath
  }

// COMMAND ----------

// MAGIC %md
// MAGIC ##Tables

// COMMAND ----------

// DBTITLE 1,Managed Table
val csvFile = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
val flightsDf = spark.read.option("header",true).schema(schema).csv(csvFile)
flightsDf.write.mode("overwrite").saveAsTable("managed_us_delay_flights_tbl")

// COMMAND ----------

// DBTITLE 1,Unmanaged Table
spark.sql("""
CREATE TABLE unmanaged_us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)
USING csv 
OPTIONS (header 'true',PATH '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')
""")

// COMMAND ----------

// DBTITLE 1,Metadata Information
println(spark.catalog.currentDatabase)
println(spark.catalog.currentCatalog)
spark.catalog.listDatabases.show()
spark.catalog.listTables.show()
spark.catalog.listColumns("global_temp.us_origin_airport_SFO_global_tmp_view").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Views

// COMMAND ----------

// DBTITLE 1,View
// MAGIC %sql
// MAGIC CREATE OR REPLACE VIEW us_origin_airport_JFK_view AS
// MAGIC  SELECT date, delay, origin, destination from managed_us_delay_flights_tbl WHERE
// MAGIC  origin = 'JFK'

// COMMAND ----------

// DBTITLE 1,Temporary View
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination from managed_us_delay_flights_tbl WHERE
// MAGIC  origin = 'JFK'

// COMMAND ----------

// DBTITLE 1,Global Temporary View
// MAGIC %sql
// MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination from managed_us_delay_flights_tbl WHERE
// MAGIC  origin = 'SFO';

// COMMAND ----------

// DBTITLE 1,Show Views
// MAGIC
// MAGIC %sql
// MAGIC show views

// COMMAND ----------

// DBTITLE 1,Global_temp
// MAGIC %sql
// MAGIC show views in global_temp

// COMMAND ----------

// DBTITLE 1,DROP - SQL
// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
// MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

// COMMAND ----------

// DBTITLE 1,DROP - Scala
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ##Cache

// COMMAND ----------

// DBTITLE 1,https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-aux-cache-cache-table.html
// MAGIC %sql
// MAGIC --CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM testData;

// COMMAND ----------

// DBTITLE 1,Tables -- DataFrames
val usFlightsDF = spark.sql("SELECT * FROM managed_us_delay_flights_tbl")
val usFlightsDF2 = spark.table("managed_us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Data Sources for Dataframes and SQL Tables

// COMMAND ----------

// MAGIC %md
// MAGIC ###DataFrameReader

// COMMAND ----------

// DataFrameReader is the core construct for reading data from a data source into a DataFrame. 
// It has a defined format and a recommended pattern for usage:
// DataFrameReader.format(args).option("key", "value").schema(args).load()
println(spark.read)
println(spark.readStream)

// COMMAND ----------

// DBTITLE 1,PARQUET
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"""
val df = spark.read.format("parquet").load(file)

// COMMAND ----------

// DBTITLE 1,CSV
// Use CSV
val df3 = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .option("mode", "PERMISSIVE")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")

// COMMAND ----------

// DBTITLE 1,JSON
// Use JSON
val df4 = spark.read.format("json")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

// COMMAND ----------

// MAGIC %md
// MAGIC ###DataFrameWriter

// COMMAND ----------

// // DataFrameWriter does the reverse of its counterpart: 
// it saves or writes data to a specified built-in data source. 
// Unlike with DataFrameReader, you access its instance not from a SparkSession but from the DataFrame you wish to save. 
// It has a few recommended usage patterns:

// DataFrameWriter.format(args)
//  .option(args)
//  .bucketBy(args)
//  .partitionBy(args)
//  .save(path)

// DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)


// COMMAND ----------

// MAGIC %md
// MAGIC ###PARQUET

// COMMAND ----------

// DBTITLE 1,Parquet File to Dataframe
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"""
val df = spark.read.format("parquet").load(file)
display(df)

// COMMAND ----------

// DBTITLE 1,Parquet File to SQL Table
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING parquet
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/" )

// COMMAND ----------

spark.sql("select * from us_delay_flights_tbl").show()

// COMMAND ----------

// DBTITLE 1,Dataframe to Parquet
df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet")

// COMMAND ----------

display(dbutils.fs.ls("/tmp/data/parquet/df_parquet"))

// COMMAND ----------

df.write
.mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet_1")

// COMMAND ----------

display(dbutils.fs.ls("/tmp/data/parquet/df_parquet_1"))

// COMMAND ----------

// DBTITLE 1,Dataframe to SQL Table
df.write
 .mode("overwrite")
 .saveAsTable("us_delay_flights_tbl_test")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from learn_spark_db.us_delay_flights_tbl_test

// COMMAND ----------

// MAGIC %md
// MAGIC ####Options - Mode

// COMMAND ----------

// DBTITLE 1,Default without Schema Diff
val diamonds_without_schema = spark.read.format("csv")
  .option("header", "true")
  .load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv").limit(40)
display(diamonds_without_schema)

// COMMAND ----------

// DBTITLE 1,PERMISSIVE
val schema = new StructType()
  .add("_c0",IntegerType,true)
  .add("carat",DoubleType,true)
  .add("cut",StringType,true)
  .add("color",StringType,true)
  .add("clarity",StringType,true)
  .add("depth",IntegerType,true) // The depth field is defined wrongly. The actual data contains floating point numbers, while the schema specifies an integer.
  .add("table",DoubleType,true)
  .add("price",IntegerType,true)
  .add("x",DoubleType,true)
  .add("y",DoubleType,true)
  .add("z",DoubleType,true)
  .add("_corrupt_record", StringType, true) // The schema contains a special column _corrupt_record, which does not exist in the data. This column captures rows that did not parse correctly.

val diamonds_with_wrong_schema_permissive  = spark.read.format("csv")
  .option("mode","PERMISSIVE")
  .schema(schema)
  .load("dbfs:/FileStore/tables/diamonds.csv")

display(diamonds_with_wrong_schema_permissive)

// COMMAND ----------

// DBTITLE 1,DROPMALFORMED
val diamonds_with_wrong_schema_drop_malformed = spark.read.format("csv")
  .option("mode","DROPMALFORMED")
  .schema(schema)
  .load("dbfs:/FileStore/tables/diamonds.csv")

display(diamonds_with_wrong_schema_drop_malformed)

// COMMAND ----------

// DBTITLE 1,FAILFAST
val diamonds_with_wrong_schema_fail_fast = spark.read.format("csv")
  .option("mode", "FAILFAST")
  .schema(schema)
  .load("dbfs:/FileStore/tables/diamonds.csv")

display(diamonds_with_wrong_schema_fail_fast)

// COMMAND ----------

// MAGIC %md
// MAGIC ###JSON

// COMMAND ----------

// DBTITLE 1,Single Line Mode
dbutils.fs.put("dbfs:/FileStore/tables/test.json", 
"""
{"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
{"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
{"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
""", 
true)

// COMMAND ----------

val testJsonData = spark.read.json("dbfs:/FileStore/tables/test.json")
display(testJsonData)

// COMMAND ----------

// DBTITLE 1,Multi Line Mode
dbutils.fs.put("dbfs:/FileStore/tables/multi-line.json", """[
    {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}},
    {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}},
    {
        "string": "string3",
        "int": 3,
        "array": [
            3,
            6,
            9
        ],
        "dict": {
            "key": "value3",
            "extra_key": "extra_value3"
        }
    }
]""", true)

val mldf = spark.read.option("multiline", "true").json("dbfs:/FileStore/tables/multi-line.json")
display(mldf)

// COMMAND ----------

// DBTITLE 1,JSON File to DataFrame
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val df = spark.read.format("json").load(file)
display(df.limit(50))

// COMMAND ----------

// DBTITLE 1,JSON File to SQL Table
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING json
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
// MAGIC  )

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from us_delay_flights_tbl LIMIT 50

// COMMAND ----------

// DBTITLE 1,Dataframe to JSON
df.write.format("json")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/json/df_json")

// COMMAND ----------

// DBTITLE 1,Dataframe to SQL Table
df.write.format("json")
 .mode("overwrite")
 .saveAsTable("df_json")

// COMMAND ----------

// MAGIC %md
// MAGIC ###CSV

// COMMAND ----------

// DBTITLE 1,CSV File to Dataframe
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
val df = spark.read.format("csv")
 .schema(schema)
 .option("header", "true")
 .option("mode", "FAILFAST") // Exit if any errors
 .option("nullValue", "") // Replace any null data with quotes
 .load(file)

display(df.limit(40))

// COMMAND ----------

// DBTITLE 1,CSV File to SQL Table
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING csv
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
// MAGIC  header "true",
// MAGIC  inferSchema "true",
// MAGIC  mode "FAILFAST"
// MAGIC  )

// COMMAND ----------

// DBTITLE 1,Dataframe to CSV
df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")

// COMMAND ----------

// DBTITLE 1,Dataframe to SQL Table
df.write.format("csv").mode("overwrite").saveAsTable("df_csv")

// COMMAND ----------


