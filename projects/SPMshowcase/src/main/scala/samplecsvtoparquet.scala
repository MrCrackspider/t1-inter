import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("CsvToParquet")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /*  testschema = new StructType()
      .add("AccountId", IntegerType, true)
      .add("ClientId", IntegerType, true)
      .add("PaymentAmt", IntegerType, true)
      .add("EnrollementAmt", IntegerType, true)
      .add("TaxAmt", IntegerType, true)
      .add("ClearAmt", IntegerType, true)
      .add("CarsAmt", IntegerType, true)
      .add("FoodAmt", IntegerType, true)
      .add("FLAmt", IntegerType, true)
      .add("CutoffDt", IntegerType, true)*/

  //read csv with options
  val dfScalaSampleaccounts = spark.read
    .options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
    .csv("docs\\samplecsv\\ScalaSampleaccounts.csv")
  dfScalaSampleaccounts.show()
  dfScalaSampleaccounts.printSchema()

/*  val dfScalaSampleclients = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
    .csv("docs\\samplecsv\\ScalaSampleclients.csv")
  dfScalaSampleclients.show()
  dfScalaSampleclients.printSchema()

  val dfScalaSampleops = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
    .csv("docs\\samplecsv\\ScalaSampleops.csv")
  dfScalaSampleops.show()
  dfScalaSampleops.printSchema()

  val dfScalaSamplecourses = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
    .csv("docs\\samplecsv\\ScalaSamplecourses.csv")
  dfScalaSamplecourses.show()
  dfScalaSamplecourses.printSchema()*/

  dfScalaSampleaccounts.write.mode(SaveMode.Overwrite).parquet("hdfs://192.168.56.1:8020/tmp/SPMscAccounts.parquet")

  //convert to parquet
  //dfScalaSampleaccounts.write.mode(SaveMode.Overwrite).parquet("docs\\sampleparquet\\ScalaSampleaccounts.parquet")
  //dfScalaSampleclients.write.mode(SaveMode.Overwrite).parquet("docs\\sampleparquet\\ScalaSampleclients.parquet")
  //dfScalaSampleops.write.mode(SaveMode.Overwrite).parquet("docs\\sampleparquet\\ScalaSampleops.parquet")
  //dfScalaSamplecourses.write.mode(SaveMode.Overwrite).parquet("docs\\sampleparquet\\ScalaSamplecourses.parquet")
}
