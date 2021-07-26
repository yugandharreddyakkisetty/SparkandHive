package com.hivepkg
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File


object HiveDemo {

  def main(args: Array[String]): Unit = {
    // Source https://spark.apache.org/docs/3.0.0/sql-data-sources-hive-tables.html
    /*
    * When working with Hive, one must instantiate SparkSession with Hive support,
    * including connectivity to a persistent Hive metastore, support for Hive serdes,
    * and Hive user-defined functions. Users who do not have an existing Hive deployment
    * can still enable Hive support. When not configured by the hive-site.xml,
    * the context automatically creates metastore_db
    * in the current directory and creates a directory configured by spark.sql.warehouse.dir,
    * which defaults to the directory spark-warehouse in the current directory that the Spark application is started.
    * use spark.sql.warehouse.dir to specify the default location of database in warehouse.
    * You may need to grant write privilege to the user who starts the Spark application.
    * if the specified warehouse has more than one db, then you must explicitly specify the while reading or writing
    * to hive
    * */

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder()
    .appName("SparkandHive")
    .master("local[*]")
    .config("spark.sql.warehouse.dir",warehouseLocation )
    .enableHiveSupport()
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import spark.sql
    val df = spark.read.format("csv").option("header",true).load("data/sample.csv")
    df.show()

    // reading data from csv file and writing it to hive table

    sql("CREATE TABLE IF NOT EXISTS hive_records(key int, value string) STORED AS PARQUET")
    df.write.format("hive").mode(SaveMode.Append).saveAsTable("hive_records")


    // if you have partitions in your table , then use the following
    // df.write.partitionBy("key").format("hive").saveAsTable("hive_records")

    // reading from hive table
    val df1=spark.table("hive_records")
    df1.show()


  }

}
