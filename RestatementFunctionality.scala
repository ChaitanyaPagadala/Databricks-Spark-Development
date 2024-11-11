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

var factvaluestable=spark.read
        .format("com.databricks.spark.csv")
        .option("header", true)
        .option("sep", ",")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("quote", "\"")
        .option("escape", "\"")
        .load("/FileStore/tables/export__31_.csv").toDF()

// COMMAND ----------

factvaluestable.drop("raw_dim0_elem_id").drop("raw_dim1_elem_id").drop("raw_dim2_elem_id").createOrReplaceTempView("FACT_VALUES_RAW")

// COMMAND ----------

val df=Seq(("74883","25230","1","1.0","13.96","8.0","8.0","10.0","58010"),("76900","25230","1","1.0","10.16","3.0","3.0","20.0","58010"),("1","2","3","4","5","6","7","30.0","8"))
val factValuesIncrementalData=df.toDF("dim0_elem_id","dim1_elem_id","data_source_id","fact_4001","fact_6481","fact_6482","fact_6483","fact_6484","dim2_elem_id")
display(factValuesIncrementalData)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from FACT_VALUES_RAW

// COMMAND ----------

val incrFileFactCols = factValuesIncrementalData.columns.filter { x => x.contains("fact_") }
val hiveColumns = spark.sql("select * from FACT_VALUES_RAW").columns.filter(_.startsWith("fact_"))
val newColumns = incrFileFactCols.diff(hiveColumns)
val primaryKeyColumns = Seq("dim0_elem_id","dim1_elem_id","dim2_elem_id","data_source_id")
val tableName = "FACT_VALUES_RAW"
val overWriteExistingData = false

// COMMAND ----------

val sourceTable = tableName
val sourceTableAlias = tableName + "_alias";
val tempTableName = tableName + "_INCREMENTAL"
factValuesIncrementalData.createOrReplaceTempView(tempTableName)
val tempTableAlias = "temp"
val staticColumns = List("DIM0_ELEM_ID","DIM1_ELEM_ID","DIM2_ELEM_ID","DATA_SOURCE_ID")
val deltaFactColumns = 
        spark.sql("select * from FACT_VALUES_RAW")
             .columns
             .filterNot(staticColumns.contains)
             .filterNot(newColumns.contains)
val tempFactColumns = 
        spark.table(tableName + "_INCREMENTAL")
             .columns
             .filterNot(staticColumns.contains)
val updateColumns =
        deltaFactColumns.map(col => s"$sourceTableAlias.$col = $sourceTableAlias.$col")
val newColumnsFromFile =
        newColumns.map(col => s"$sourceTableAlias.$col = $tempTableAlias.$col")
val updateColumnsOverwrite =
        tempFactColumns.map(col => s"$sourceTableAlias.$col = $tempTableAlias.$col")
val mergeCondition = primaryKeyColumns
        .map(c => s"$sourceTableAlias.$c = $tempTableAlias.$c")
        .mkString(" AND ")
val udpateQueryPart = {
        var query = Array[String]()
        if (overWriteExistingData) {
          query = updateColumnsOverwrite ++ newColumnsFromFile
        } else {
          query = updateColumns ++ newColumnsFromFile
        }
        query
      }
val mergeQueryPart =
        s"""WHEN MATCHED THEN
      UPDATE SET
        ${udpateQueryPart.mkString(",\n")}
        """
val mergeQuery =
        s"""
      MERGE INTO $sourceTable $sourceTableAlias
      USING $tempTableName $tempTableAlias
      ON $mergeCondition
      $mergeQueryPart
      WHEN NOT MATCHED
        THEN INSERT (${(primaryKeyColumns ++ tempFactColumns).mkString(", ")})
          VALUES (${(primaryKeyColumns.map(c => s"$tempTableAlias.$c") ++ tempFactColumns
          .map(c => s"$tempTableAlias.$c")).mkString(", ")})
    """

// COMMAND ----------



// COMMAND ----------

val df=Seq(("76924","76926","76914","74883","25230","1","1.0","13.96","8.0","8.0","58010"),("76928","76926","76914","76900","25230","1","1.0","10.16","3.0","3.0","58010"),("1","2","3","4","5","6","7","8","9","10","11"))
factvaluestable=df.toDF("RAW_DIM0_ELEM_ID","RAW_DIM1_ELEM_ID","RAW_DIM2_ELEM_ID","dim0_elem_id","dim1_elem_id","data_source_id","fact_4001","fact_6481","fact_6482","fact_6483","dim2_elem_id")

// COMMAND ----------

display(factvaluestable)

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

factvaluestable.createOrReplaceTempView("fact_values_restatement_entries")
val unitsAndPromoFacts: ListBuffer[String] = ListBuffer[String]()
val unitsAndPromoRawIds = List(6482,43567)
    factvaluestable.
      columns.
      filter { x => x.contains("fact_") }.
      map {
        x => x.split("fact_")(1).toLong }.
      foreach(x => if (unitsAndPromoRawIds.contains(x)) unitsAndPromoFacts += "fact_" + x)

val unitsAndPromoFactsCondition = unitsAndPromoFacts.map(x => x + " IS NOT null").mkString(" OR ")
val hasUnitsOrPromoClause = if (unitsAndPromoFactsCondition.length > 0)  s"(CASE WHEN ${unitsAndPromoFactsCondition} THEN 'Y' ELSE 'N' END)"  else  s"'N'"

val instanceId="1111"
val taskId="3540"
val dataSourceId="1"
val hiveSchemaName="ap_dev_csrp_9600"
var restated_dims_query="" 
val tableName = "parquet_restated_dims_"+dataSourceId.toString()

val restatedDimensions = spark.sql(s"""SELECT
                                     DIM0_ELEM_ID,
                                     DIM1_ELEM_ID,
                                     DIM2_ELEM_ID,
                                     RAW_DIM0_ELEM_ID,
                                     RAW_DIM1_ELEM_ID,
                                     RAW_DIM2_ELEM_ID,                                   
                                     $hasUnitsOrPromoClause AS HAS_UNITS_OR_PROMO,
                                     'Y' AS HAS_OTHER_FACTS,
                                     'N' AS PROCESSING_STATUS,
                                     CAST(${taskId} AS BIGINT) AS TASK_ID,
                                     CURRENT_TIMESTAMP AS CREATED_TS,
                                     CAST(NULL AS TIMESTAMP) AS UPDATED_TS,
                                     CAST(${instanceId} AS STRING) AS CREATED_BY,
                                     CAST(null AS STRING) AS UPDATED_BY
                                  FROM fact_values_restatement_entries""")

spark.sqlContext.sql("create schema if not exists " + hiveSchemaName);


// COMMAND ----------

val tableExist = spark.catalog.tableExists(hiveSchemaName, tableName);

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from ap_dev_csrp_9600.parquet_restated_dims_1

// COMMAND ----------

display(restatedDimensions)

// COMMAND ----------

restatedDimensions.printSchema()

// COMMAND ----------

display(restatedDimensions.as("file").join(spark.read.table(hiveSchemaName + "." + tableName).as("table"),Seq("dim0_elem_id", "dim1_elem_id", "dim2_elem_id"),"full_outer"))

// COMMAND ----------

display(restatedDimensions.as("file").join(spark.read.table(hiveSchemaName + "." + tableName).as("table"),Seq("dim0_elem_id", "dim1_elem_id", "dim2_elem_id"),"full_outer").selectExpr("dim0_elem_id","dim1_elem_id","dim2_elem_id",
            "CASE WHEN file.RAW_DIM0_ELEM_ID IS NULL THEN table.RAW_DIM0_ELEM_ID ELSE file.RAW_DIM0_ELEM_ID END AS RAW_DIM0_ELEM_ID",
            "CASE WHEN file.RAW_DIM1_ELEM_ID IS NULL THEN table.RAW_DIM1_ELEM_ID ELSE file.RAW_DIM1_ELEM_ID END AS RAW_DIM1_ELEM_ID",
            "CASE WHEN file.RAW_DIM2_ELEM_ID IS NULL THEN table.RAW_DIM2_ELEM_ID ELSE file.RAW_DIM2_ELEM_ID END AS RAW_DIM2_ELEM_ID",
            """CASE WHEN file.has_units_or_promo IS NULL THEN table.has_units_or_promo
                 WHEN file.has_units_or_promo IS NOT NULL AND table.has_units_or_promo IS NOT NULL
                     THEN  CASE WHEN file.has_units_or_promo ='Y' OR table.has_units_or_promo = 'Y' THEN 'Y'
                                ELSE 'N'
                            END
                 WHEN table.has_units_or_promo IS NULL THEN file.has_units_or_promo
                 ELSE 'N'
            END as has_units_or_promo""",
            "CASE WHEN file.processing_status IS NULL THEN table.processing_status ELSE file.processing_status END AS processing_status",
            "CASE WHEN file.task_id IS NULL THEN table.task_id ELSE file.task_id END AS task_id",
            "CASE WHEN table.created_ts IS NULL THEN file.created_ts ELSE table.created_ts END AS created_ts",
            "CASE WHEN table.created_by IS NULL THEN file.created_by ELSE table.created_by END AS created_by",
            s"""CASE WHEN file.created_by IS NOT NULL AND table.created_by IS NOT NULL THEN ${instanceId}
                   WHEN file.created_by IS NULL THEN table.updated_by ELSE file.updated_by END AS updated_by""",
            """CASE WHEN table.created_ts IS NOT NULL AND file.created_ts IS NOT NULL THEN CURRENT_TIMESTAMP
                    WHEN file.created_ts IS NULL THEN table.updated_ts ELSE file.updated_ts END AS updated_ts""",
            "CASE WHEN file.has_other_facts IS NULL THEN table.has_other_facts ELSE file.has_other_facts END AS has_other_facts"))

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from ap_dev_csrp_9600.parquet_restated_dims_1

// COMMAND ----------



// COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
try {
      if (tableExist) {
        println("Inserting into the existing restated dims Table")
        val newRestatedDimensions =restatedDimensions.as("file").join(spark.read.table(hiveSchemaName + "." + tableName).as("table"),Seq("dim0_elem_id", "dim1_elem_id", "dim2_elem_id"),"full_outer").selectExpr("dim0_elem_id","dim1_elem_id","dim2_elem_id",
  """CASE WHEN file.has_units_or_promo IS NULL THEN table.has_units_or_promo
       WHEN file.has_units_or_promo IS NOT NULL AND table.has_units_or_promo IS NOT NULL
           THEN  CASE WHEN file.has_units_or_promo ='Y' OR table.has_units_or_promo = 'Y' THEN 'Y'
                      ELSE 'N'
                  END
       WHEN table.has_units_or_promo IS NULL THEN file.has_units_or_promo
       ELSE 'N'
  END as has_units_or_promo""",
  "CASE WHEN file.processing_status IS NULL THEN table.processing_status ELSE file.processing_status END AS processing_status",
  "CASE WHEN file.task_id IS NULL THEN table.task_id ELSE file.task_id END AS task_id",
  "CASE WHEN table.created_ts IS NULL THEN file.created_ts ELSE table.created_ts END AS created_ts",
  """CASE WHEN table.created_ts IS NOT NULL AND file.created_ts IS NOT NULL THEN CURRENT_TIMESTAMP
                    WHEN file.created_ts IS NULL THEN table.updated_ts ELSE file.updated_ts END AS updated_ts""",
  "CASE WHEN table.created_by IS NULL THEN file.created_by ELSE table.created_by END AS created_by",
  s"""CASE WHEN file.created_by IS NOT NULL AND table.created_by IS NOT NULL THEN ${instanceId}
         WHEN file.created_by IS NULL THEN table.updated_by ELSE file.updated_by END AS updated_by""",
  "CASE WHEN file.has_other_facts IS NULL THEN table.has_other_facts ELSE file.has_other_facts END AS has_other_facts")
        newRestatedDimensions.
          write.
          option("partitionOverwriteMode", "dynamic").
          mode(SaveMode.Overwrite).
          insertInto(hiveSchemaName + "." + tableName)
        println("Inserting into the existing restated dims Table Completed.No error Occured")
      } else {
        println("we are not having restated dims table in this schema we are creating it")
        restatedDimensions.
          write.
          format("parquet").
          mode(SaveMode.Overwrite).
          partitionBy("has_other_facts").option("path", getHiveTableLocation(hiveSchemaName, tableName)).saveAsTable(hiveSchemaName + "." + tableName)
        println("Inserted into a restated dims table with new row in hive schema")
      }
    }
    catch {
      case e: Exception =>{
          throw e
        }
    }

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from ap_dev_csrp_9600.parquet_restated_dims_1

// COMMAND ----------

display(restatedDimensions)

// COMMAND ----------

// factvaluestable=factvaluestable.withColumn("has_units_or_promo",when($"fact_6484".isNull or $"fact_6484" === "", lit(null)).otherwise("Y"))
// val unitsAndPromoFacts: List[String] = List("fact_6482", "fact_18051")
// factvaluestable=factvaluestable.withColumn("has_units_or_promo",
//                                            when(lit(unitsAndPromoFacts.length)===0,lit(null)).otherwise(
//                                              when(col(unitsAndPromoFacts(0)).isNull or col(unitsAndPromoFacts(0)) === "", lit(null)).otherwise("Y"))
//                                           )

// COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/ap_dev_csrp_9600.db/parquet_restated_dims_1",true)

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table ap_dev_csrp_9600.parquet_restated_dims_1

// COMMAND ----------

// MAGIC %md # DELTA MERGE TABLES

// COMMAND ----------

// MAGIC %python
// MAGIC from delta.tables import *
// MAGIC from pyspark.sql.functions import *

// COMMAND ----------

// olddf=/user/hive/warehouse/ap_dev_csrp_9600.db/restated_dims_1
// newdf=/user/hive/warehouse/ap_dev_csrp_9600.db/new_restated_dims_1

restatedDimensions.write.mode("overwrite").format("delta").option("path","/user/hive/warehouse/ap_dev_csrp_9600.db/delta_restated_dims_1").saveAsTable(hiveSchemaName + ".delta_restated_dims_1")
restatedDimensions.write.mode("overwrite").format("delta").option("path","/user/hive/warehouse/ap_dev_csrp_9600.db/delta_new_restated_dims_1").saveAsTable(hiveSchemaName + ".delta_new_restated_dims_1")

// COMMAND ----------

// MAGIC %python
// MAGIC old_deltaTable = DeltaTable.forPath(spark, "/user/hive/warehouse/ap_dev_csrp_9600.db/delta_restated_dims_1")
// MAGIC deltaTable = DeltaTable.forPath(spark, "/user/hive/warehouse/ap_dev_csrp_9600.db/delta_new_restated_dims_1")

// COMMAND ----------

display(spark.read.format("delta").load("/user/hive/warehouse/ap_dev_csrp_9600.db/delta_restated_dims_1"))

// COMMAND ----------

display(spark.read.format("delta").load("/user/hive/warehouse/ap_dev_csrp_9600.db/delta_new_restated_dims_1"))

// COMMAND ----------

// MAGIC %python
// MAGIC old_deltaTable.alias("oldData").merge(
// MAGIC     source = deltaTable.alias("newData").toDF(),
// MAGIC     condition = "oldData.DIM0_ELEM_ID = newData.DIM0_ELEM_ID AND oldData.DIM1_ELEM_ID = newData.DIM1_ELEM_ID AND oldData.DIM2_ELEM_ID = newData.DIM2_ELEM_ID AND oldData.HAS_UNITS_OR_PROMO='N'" 
// MAGIC   ).whenMatchedUpdate(set =
// MAGIC     {
// MAGIC       "oldData.HAS_UNITS_OR_PROMO" : "newData.HAS_UNITS_OR_PROMO",
// MAGIC       "oldData.HAS_OTHER_FACTS" : "newData.HAS_OTHER_FACTS",
// MAGIC       "oldData.PROCESSING_STATUS" : "newData.PROCESSING_STATUS",
// MAGIC       "oldData.TASK_ID" : "newData.TASK_ID",
// MAGIC       "oldData.CREATED_TS" : "newData.CREATED_TS",
// MAGIC       "oldData.CREATED_BY" : "newData.CREATED_BY",
// MAGIC       "oldData.UPDATED_BY" : "newData.UPDATED_BY"
// MAGIC     }).whenNotMatchedInsert(values =
// MAGIC     {
// MAGIC       "oldData.DIM0_ELEM_ID" : "newData.DIM0_ELEM_ID",
// MAGIC       "oldData.DIM1_ELEM_ID" : "newData.DIM1_ELEM_ID",
// MAGIC       "oldData.DIM2_ELEM_ID" : "newData.DIM2_ELEM_ID",
// MAGIC       "oldData.HAS_UNITS_OR_PROMO" : "newData.HAS_UNITS_OR_PROMO",
// MAGIC       "oldData.HAS_OTHER_FACTS" : "newData.HAS_OTHER_FACTS",
// MAGIC       "oldData.PROCESSING_STATUS" : "newData.PROCESSING_STATUS",
// MAGIC       "oldData.TASK_ID" : "newData.TASK_ID",
// MAGIC       "oldData.CREATED_TS" : "newData.CREATED_TS",
// MAGIC       "oldData.CREATED_BY" : "newData.CREATED_BY",
// MAGIC       "oldData.UPDATED_BY" : "newData.UPDATED_BY"
// MAGIC     }
// MAGIC   ).execute()

// COMMAND ----------



// COMMAND ----------

File uploaded to /FileStore/tables/export__32_.csv
File uploaded to /FileStore/tables/export__33_.csv

// COMMAND ----------

spark.read
        .format("com.databricks.spark.csv")
        .option("header", true)
        .option("sep", ",")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("quote", "\"")
        .option("escape", "\"")
        .load("/FileStore/tables/export__32_.csv").createOrReplaceTempView("TEMP_AGG_DATA")

spark.read
        .format("com.databricks.spark.csv")
        .option("header", true)
        .option("sep", ",")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("quote", "\"")
        .option("escape", "\"")
        .load("/FileStore/tables/export__33_.csv").createOrReplaceTempView("TEMP_MASTER_DATA")

spark.read
        .format("com.databricks.spark.csv")
        .option("header", true)
        .option("sep", "\t")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("quote", "\"")
        .option("escape", "\"")
        .load("/FileStore/tables/Restated_dims-1").createOrReplaceTempView("RESTATED")

// COMMAND ----------

// MAGIC %sql
// MAGIC select dim2_elem_id,sum(FACT_4000) as VALUE_SALES,sum(FACT_4002) as UNIT_SALES,sum(FACT_4004) as STORES_SELLING,sum(FACT_4014) as UNIT_SALES_CU from TEMP_AGG_DATA group by dim2_elem_id

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from TEMP_MASTER_DATA

// COMMAND ----------

val Restated_table=spark.sql("select * from RESTATED r where  r.HAS_OTHER_FACTS = 'Y' and r.DIM2_ELEM_ID<0").toDF()

// COMMAND ----------

display(Restated_table)

// COMMAND ----------

val updQuery="""SELECT 
CASE WHEN df1.IS_NEW=true then df1.DIM2_ELEM_ID else df2.DIM2_ELEM_ID END as DIM2_ELEM_ID,
CASE WHEN df1.IS_NEW=true then df1.DIM1_ELEM_ID else df2.DIM1_ELEM_ID END as DIM1_ELEM_ID,
CASE WHEN df1.IS_NEW=true then df1.DIM0_ELEM_ID else df2.DIM0_ELEM_ID END as DIM0_ELEM_ID,
CASE WHEN df1.IS_NEW=true then df1.FORECAST_UPPER_BOUND else df2.FORECAST_UPPER_BOUND END as FORECAST_UPPER_BOUND,
CASE WHEN df1.IS_NEW=true then df1.FORECAST_LOWER_BOUND else df2.FORECAST_LOWER_BOUND END as FORECAST_LOWER_BOUND,
df2.FORECASTED_BASELINE_UNITS,
df2.REG_PRICE,
df2.TRUE_BASELINE_UNITS,
df2.ITEM_IS_ON_PROMO,
df2.OOS_CONFIDENCE,
df2.BASELINE_UNITS,
CASE WHEN df1.IS_NEW=true then df1.DATA_SOURCE_ID else df2.DATA_SOURCE_ID END as DATA_SOURCE_ID,
CASE WHEN df1.IS_NEW=true then df1.FACT_4014 else df2.FACT_4014 END as FACT_4014,
CASE WHEN df1.IS_NEW=true then df1.FACT_4002 else df2.FACT_4002 END as FACT_4002,
CASE WHEN df1.IS_NEW=true then df1.FACT_4000 else df2.FACT_4000 END as FACT_4000,
CASE WHEN df1.IS_NEW=true then df1.FACT_4004 else df2.FACT_4004 END as FACT_4004 
FROM TEMP_AGG_DATA df1 full outer JOIN TEMP_MASTER_DATA df2 
ON df1.DIM0_ELEM_ID = df2.DIM0_ELEM_ID AND df1.DIM1_ELEM_ID = df2.DIM1_ELEM_ID AND df1.DIM2_ELEM_ID = df2.DIM2_ELEM_ID"""

// COMMAND ----------

import org.apache.spark.sql.functions.col
// FULL DATA AGGREGATION FOR BOTH DATA SHARING AND OSA CLIENTS
val solutionType = "DATA SHARING"
val updatedMasterDf = if(solutionType.nonEmpty && solutionType.toUpperCase()=="OSA") 
println("updatedMasterDf for OSA service type")
spark.sql(s"""With updatedMasterDfTemp as (${updQuery})
select * from updatedMasterDfTemp where 
DIM0_ELEM_ID not in (select DIM0_ELEM_ID from RESTATED r where r.HAS_OTHER_FACTS = 'Y' and r.DIM2_ELEM_ID<0)
AND
DIM1_ELEM_ID not in (select DIM1_ELEM_ID from RESTATED r where r.HAS_OTHER_FACTS = 'Y' and r.DIM2_ELEM_ID<0)""").filter(col("DIM0_ELEM_ID").isNotNull).filter(col("DIM1_ELEM_ID").isNotNull).filter(col("DIM2_ELEM_ID").isNotNull) 
else println("updatedMasterDf for DATA SHARING service type") aggrDf

// COMMAND ----------

updatedMasterDf.createOrReplaceTempView("updatedMasterDf")

// COMMAND ----------

spark.sql("select * except(has_units_or_promo,has_other_facts) from RESTATED")

// COMMAND ----------

spark.sql(updQuery).filter(col("DIM0_ELEM_ID").isNotNull).filter(col("DIM1_ELEM_ID").isNotNull).filter(col("DIM2_ELEM_ID").isNotNull).createOrReplaceTempView("chay")

// COMMAND ----------

display(spark.sql(updQuery))

// COMMAND ----------

// MAGIC %sql
// MAGIC select dim2_elem_id,sum(FACT_4000) as VALUE_SALES,sum(FACT_4002) as UNIT_SALES,sum(FACT_4004) as STORES_SELLING,sum(FACT_4014) as UNIT_SALES_CU from chay where dim0_elem_id in (4007,4008,4009) group by dim2_elem_id 

// COMMAND ----------

display(spark.sql("select df1.*,df2.* from TEMP_AGG_DATA df1 full outer JOIN TEMP_MASTER_DATA df2 ON df1.DIM0_ELEM_ID = df2.DIM0_ELEM_ID AND df1.DIM1_ELEM_ID = df2.DIM1_ELEM_ID AND df1.DIM2_ELEM_ID = df2.DIM2_ELEM_ID"))

// COMMAND ----------

/FileStore/tables/Restated_dims-1

// COMMAND ----------

display(spark.sql("""with chay as (select posexplode(split(space(5),' ')))
select date_add('2022-01-01',pos) from chay
"""))

// COMMAND ----------


