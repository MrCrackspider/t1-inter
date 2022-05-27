package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TaxiCSV2Parquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("TaxiCSV2Parquet")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val schema = new StructType()
    .add("VendorID", IntegerType, true)
    .add("tpep_pickup_datetime", TimestampType, true)
    .add("tpep_dropoff_datetime", TimestampType, true)
    .add("passenger_count", IntegerType, true)
    .add("trip_distance", DoubleType, true)
    .add("RatecodeID", IntegerType, true)
    .add("store_and_fwd_flag", StringType, true)
    .add("PULocationID", IntegerType, true)
    .add("DOLocationID", IntegerType, true)
    .add("payment_type", IntegerType, true)
    .add("fare_amount", DoubleType, true)
    .add("extra", DoubleType, true)
    .add("mta_tax", DoubleType, true)
    .add("tip_amount", DoubleType, true)
    .add("tolls_amount", DoubleType, true)
    .add("improvement_surcharge", DoubleType, true)
    .add("total_amount", DoubleType, true)
    .add("congestion_surcharge", DoubleType, true)

  val dfTaxi = spark.read.options(Map("delimiter"->",","header"->"true")).schema(schema)
    .csv("docs\\taxidata\\taxicsv")
  dfTaxi.show()
  dfTaxi.printSchema()

  val dfTaxiFiltered = dfTaxi
    .withColumn("pickup_date", date_trunc("month", col("tpep_pickup_datetime")))
    .where(col("pickup_date")
      .between(to_date(lit("2021-01-01")), to_date(lit("2021-07-01"))))

  dfTaxiFiltered.show()

  //convert to parquet
  dfTaxiFiltered.write.mode(SaveMode.Overwrite)
    .partitionBy("pickup_date")
    .parquet("docs\\taxidata\\taxiparquet\\Taxidata.parquet")

/*  //convert lookup to parquet
  val locations = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("docs\\taxidata\\taxi_zone_lookup.csv")
  locations.show()
  locations.printSchema()

  locations.write.mode(SaveMode.Overwrite).parquet("docs\\taxidata\\locations.parquet")*/
}
