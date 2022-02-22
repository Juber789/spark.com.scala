package AllaboutScala

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.streaming._

import java.util.Properties
object ms1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ms1").enableHiveSupport().getOrCreate()
//        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val path = "E:\\BigData\\datasets\\customers.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("quote","\'").load(path)
    val df1 = spark.read.option("header","true").option("inferSchema","true").csv(path)
    //    df.show(5,false)
    df1.show(5,false)

    val cols = df.columns.map(x=>x.toLowerCase.replaceAll("[^a-zA-Z0-9]",""))
    val ndf = df.toDF(cols:_*)
    ndf.show(4,false)

    spark.stop()
  }
}







