// Databricks notebook source
// MAGIC %fs
// MAGIC ls /FileStore/

// COMMAND ----------


dbutils.fs.ls("/FileStore/ChayCloudFiles")

// COMMAND ----------

// MAGIC %sql
// MAGIC use default;
// MAGIC CREATE TABLE IF NOT EXISTS Product (
// MAGIC  Name STRING,
// MAGIC  ProductSpecification STRING,
// MAGIC  ProductDetails STRING,
// MAGIC  Value STRING,
// MAGIC  Count STRING 
// MAGIC )
// MAGIC USING delta
// MAGIC location '/FileStore/Product'

// COMMAND ----------

// MAGIC %python
// MAGIC columns = ["ProductName", "ProductDesc", "ProductValue", "ProductCount","Name", "ProductSpecification", "ProductDetails", "Value", "Count"]
// MAGIC schema_generation = ",".join([f'{{"name":"{col}","type":"string","nullable":true,"metadata":{{}}}}' for col in columns])
// MAGIC jsonSchema = f"""{{"type":"struct","fields":[{schema_generation}]}}""".replace('"','\\"')
// MAGIC print(jsonSchema)

// COMMAND ----------

import org.json.JSONObject
import scala.collection.JavaConverters._
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType, ArrayType, TimestampNTZType, TimestampType, DateType, IntegerType, DecimalType, DataType}
import org.json.{JSONArray, JSONObject}

val combinedSchemas:String = "{\"schemas\": [{\"type\": \"struct\", \"fields\": [{\"name\": \"ProductName\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}},{\"name\": \"ProductDesc\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}}]},{\"type\": \"struct\", \"fields\": [{\"name\": \"Name1\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}},{\"name\": \"Desc1\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}}]}]}"


// Create JSONObject from the input string
val jsonObject = new JSONObject(combinedSchemas)

// Extract the schemas array
val schemasArray = jsonObject.getJSONArray("schemas")

// Iterate over the schemas and print their fields
val schema = schemasArray.getJSONObject(0).toString
val customSchema = DataType.fromJson(schema).asInstanceOf[StructType]

// COMMAND ----------

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions, streaming}
import org.apache.commons.text.StringSubstitutor
import org.apache.spark.sql.functions.{current_timestamp,element_at,input_file_name, lit, md5, reverse, split, col, when, to_date, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType, ArrayType, TimestampNTZType, TimestampType, DateType, IntegerType, DecimalType, DataType}
import org.json.{JSONArray, JSONObject}

val schemaName = "default"
val tableName = "Product"
val inputLocation = "/FileStore/ChayCloudFiles"
val checkpointLocation = "/FileStore/Product/_checkpoint"

val expectedColumns = Seq("Name","ProductSpecification","ProductDetails","Value","Count")

val configString: String = """{
  \"feedMappings\":{\"feed_A\": \"feedA_%.csv\",\"feed_B\": \"feedB_%.csv\"},\"schemaMappings\":{
    \"feed_A\": 
    {\"ProductName\":\"Name\",
    \"ProductDesc\":\"ProductSpecification\",
    \"ProductValue\":\"Value\",
    \"ProductCount\":\"Count\"},
    \"feed_B\": 
    {\"Name\":\"Name\",
    \"ProductSpecification\":\"ProductSpecification\",
    \"ProductDetails\":\"ProductDetails\",
    \"Value\":\"Value\",
    \"Count\":\"Count\"
    }}}""".replace("\\\"","\"")

val jsonObject = new JSONObject(configString)
val feedMappings = jsonObject.getJSONObject("feedMappings").keySet().toArray.map(key => key.toString -> jsonObject.getJSONObject("feedMappings").getString(key.toString)).toMap
val schemaMappings = jsonObject.getJSONObject("schemaMappings").keySet().toArray.map(key => key.toString -> {
  val innerMap = jsonObject.getJSONObject("schemaMappings").getJSONObject(key.toString)
  innerMap.keySet().toArray.map(innerKey => innerKey.toString -> innerMap.getString(innerKey.toString)).toMap
}).toMap

val jsonSchema = """{"type":"struct","fields":[
  {"name":"ProductName","type":"string","nullable":true,"metadata":{}},{"name":"ProductDesc","type":"string","nullable":true,"metadata":{}},{"name":"ProductValue","type":"string","nullable":true,"metadata":{}},{"name":"ProductCount","type":"string","nullable":true,"metadata":{}},{"name":"Name","type":"string","nullable":true,"metadata":{}},{"name":"ProductSpecification","type":"string","nullable":true,"metadata":{}},{"name":"ProductDetails","type":"string","nullable":true,"metadata":{}},{"name":"Value","type":"string","nullable":true,"metadata":{}},{"name":"Count","type":"string","nullable":true,"metadata":{}}]}"""

val schema: String = jsonSchema.replace("\\\"","\"")
val customSchema = DataType.fromJson(schema).asInstanceOf[StructType]

val df = spark.readStream.format("cloudFiles")
              .option("cloudFiles.format","csv")
              .option("header",true)
              .option("rescuedDataColumn","_rescued_data")
              .schema(customSchema)
              .load(inputLocation)
              .withColumn("file_name",input_file_name())

// Function to apply dynamic column mapping while avoiding ambiguity
def mapColumns(df: DataFrame, feedType: String): DataFrame = {
  schemaMappings.get(feedType) match {
    case Some(mapping) =>
      val existingCols = df.columns.toSet
      val renamedCols = mapping.filterKeys(existingCols.contains)
        .map { case (srcCol, tgtCol) => col(srcCol).alias(tgtCol) }
      val finalDf = feedType match {
        case "feed_A" =>
          df.select(renamedCols.toSeq:_*)
            .withColumn("ProductDetails",lit(col("ProductSpecification")))
        case _ =>
          df.select(renamedCols.toSeq:_*)
      }
    finalDf
    case None => df
  }
}

// // Process all feeds and union them into a single DataFrame
val transformedDF = feedMappings.keys.map { feedType =>
  val pattern = feedMappings(feedType)
  val dfFiltered = df.filter(col("file_name").like(s"%$pattern%"))
  mapColumns(dfFiltered, feedType).select(expectedColumns.map(col):_*)
}.reduce(_ union _)

// COMMAND ----------

display(df)

// COMMAND ----------

display(transformedDF)

// COMMAND ----------


