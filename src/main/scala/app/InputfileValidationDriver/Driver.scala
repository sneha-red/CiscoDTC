package app.InputfileValidationDriver

import app.SqlPlacementHandler.SqlHandler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.io.FileNotFoundException

object Driver {
  var sqlHandler = new  SqlHandler
  def main(args: Array[String]): Unit = {
     val spark: SparkSession = SparkSession.builder().master("local").appName("Validate_JSON_Files").getOrCreate()

     //Custom Schema. This is required only when a certain sequence of schema is required. Spark inherently can define schema and data types for each input element/column.
     val inputSchema = new StructType(Array(
       StructField("bg",StringType, true),
       StructField("dp",IntegerType,true),
       StructField("nm",StringType, true),
       StructField("ph",StringType,true),
       StructField("pt",LongType, true),
       StructField("sha",StringType,true),
       StructField("si",StringType, true),
       StructField("ts",LongType,true),
       StructField("uu",StringType, true)))

    try {
      /*reading the input JSON file to a dataframe with custom schema. persist() is ideal for large files*/
      val inputJson = spark.read.schema(inputSchema).json("spark-warehouse/files_metadata.json").persist()
      println("input JSON Objects count: " + inputJson.count())
      inputJson.printSchema()
      inputJson.show(false)

      /*adding extra field to the input records to identify the extension of the file - 'file_ext' */
      val enrichedJsonInput = inputJson.withColumn("file_ext", split(col("nm"), "\\.").getItem(1)).persist()
      enrichedJsonInput.createOrReplaceTempView("tmp_enriched_json")
      inputJson.unpersist() /*All dataframe persist and unpersist are set for an optimized execution flow keeping in view large files */
      println("enriched JSON Objects count: " + enrichedJsonInput.count())
      enrichedJsonInput.printSchema()
      enrichedJsonInput.show(false)

      /* Extracting file_extentions and unique file count for each extension*/
      val uniqueFileExtension = spark.sql(sqlHandler.extractUniqueExtensions)
      /* All persisted dfs will be unpersisted by spark before cluster is shutdown. unpersist is used only when a df is need no more in the long sequence of executions as an optimization practice */
      enrichedJsonInput.unpersist()
      println("uniqueFileExtensions count: " + uniqueFileExtension.count())
      uniqueFileExtension.show(false)

      uniqueFileExtension.collect().foreach(t => println(t(0) + ": " + t(1)))
      println("========================output ends here=============================")
    } catch{
      case ex: FileNotFoundException => {
        println("Invalid input, Please check that input has a valid file")
      throw new Exception(ex )
      }
      case ex1: Exception => {
        println("Unknown error, try again!")
        throw new Exception(ex1 )
      }
    } finally {
      println("Execution completed, Please scroll through console/logs for execution information and output!  ")
    }
    }
}
