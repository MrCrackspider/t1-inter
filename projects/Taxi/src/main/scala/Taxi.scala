package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Taxi extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Taxi")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  var taxidata = spark.read.parquet("docs\\taxidata\\taxiparquet\\Taxidata.parquet")

  taxidata.printSchema()
  taxidata.show()


/**Нйти топ 10 самых популярных точек назначения.
При этом не учитываем поездки, котроые попадают в 10% самых дорогих или дешевых в расчете на километр пути для каждой точки.

В результате вы должны получить таблицу вида:
Id места назначения, количество поездок, средняя цена за километр пути*/

  var ths = taxidata
    .withColumn("AmountPerKm", col("total_amount") / (col("trip_distance") * 1.6093))
    .stat.approxQuantile("AmountPerKm", Array(0.1, 0.9), 0.01)
  println(ths(0), ths(1))

    taxidata.withColumn("AmountPerKm", col("total_amount") / (col("trip_distance") * 1.6093))
      .filter(col("AmountPerKm") > ths(0) and col("AmountPerKm") < ths(1))
      .groupBy(col("DOLocationID"))
      .agg(count("DOLocationID").as("TripCount"), round(avg("AmountPerKm"), 2).as("AvgAmountPerKm"))
      .orderBy(col("TripCount").desc).limit(10).show()

/*    taxidata.write.mode(SaveMode.Overwrite)
      .partitionBy("???")
      .parquet("docs\\taxidata\\taxiparquet\\task1.parquet")*/
}