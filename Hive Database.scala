// Databricks notebook source
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import sqlContext.implicits._ // for `toDF` and $""
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType,StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable._
import java.util.Arrays;

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

spark.conf.getOption("spark.sql.warehouse.dir").get

// COMMAND ----------

dbutils.fs.help()

// COMMAND ----------

for(i<-dbutils.fs.ls("/FileStore/tables/"))
{
println(i)
}

// COMMAND ----------

var factvaluestable=spark.read
        .format("com.databricks.spark.csv")
        .option("header", true)
        .option("sep", ",")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("quote", "\"")
        .option("escape", "\"")
        .load("/FileStore/tables/export__31_.csv").toDF()

// COMMAND ----------

display(factvaluestable)

// COMMAND ----------

display(spark.read
        .format("com.databricks.spark.csv")
        .option("header", false)
        .option("sep", ",")
        .load("/FileStore/tables/Chay.csv").toDF("a","b","c"))

// COMMAND ----------

val a=Seq(("1","2","3"),("4","5","6"),("7","8","9")).toDF("a","b","c")
display(a)

// COMMAND ----------

val df=Seq(("74883","25230","1","1.0","13.96","8.0","8.0","10.0","58010"),("76900","25230","1","1.0","10.16","3.0","3.0","20.0","58010"),("1","2","3","4","5","6","7","30.0","8"))
val factValuesIncrementalData=df.toDF("dim0_elem_id","dim1_elem_id","data_source_id","fact_4001","fact_6481","fact_6482","fact_6483","fact_6484","dim2_elem_id")

// COMMAND ----------

display(factValuesIncrementalData)

// COMMAND ----------

factValuesIncrementalData.columns.filter { x => x.contains("fact_") }

// COMMAND ----------

factvaluestable.createOrReplaceTempView("FACT_VALUES_RAW")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from FACT_VALUES_RAW

// COMMAND ----------

val incrFileFactCols = factValuesIncrementalData.columns.filter { x => x.contains("fact_") }
val hiveColumns = spark.sql("select * from FACT_VALUES_RAW").columns.filter(_.startsWith("fact_"))
val newColumns = incrFileFactCols.diff(hiveColumns)

// COMMAND ----------

val hiveSchemaName="goutham"
val tableName="chay"

// COMMAND ----------

spark.sqlContext.sql("create schema if not exists " + hiveSchemaName);

// COMMAND ----------

spark.catalog.tableExists(hiveSchemaName, tableName);

// COMMAND ----------



// COMMAND ----------

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

factValuesIncrementalData.write.
          format("parquet").
          mode(SaveMode.Overwrite).
          partitionBy("dim2_elem_id").option("path", getHiveTableLocation(hiveSchemaName, tableName)).saveAsTable(hiveSchemaName + "." + tableName)

// COMMAND ----------

display(spark.read.table("goutham.chay"))

// COMMAND ----------

for(i<-dbutils.fs.ls("dbfs:/user/hive/warehouse/goutham.db/chay"))
{
  println(i)
}

// COMMAND ----------

display(spark.read.parquet("dbfs:/user/hive/warehouse/goutham.db/chay/dim2_elem_id=/"))

// COMMAND ----------


