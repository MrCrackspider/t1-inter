import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties
import java.net.URI

import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, LogManager}

import scala.collection.immutable.Stream

object ParquetToPostgre extends App {
  override def main(args: Array[String]) {

    def getJsonValue(parsedJson: JValue, key: String): String = {
      val JString(value) = parsedJson \ key
      value
    }

    val spark: SparkSession = SparkSession.builder()
      //.master("local[4]")
      .appName("SPMshowcase")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    var DocsPath = "C:\\Users\\apolivanov\\stajirovka\\apolivanov\\projects\\SPMshowcase\\docs\\"
    if (args.length >= 1)
    {
      DocsPath = args(0)
    }
    else
    {
      log.warn("Docs path should be given as first argument")
      //sys.exit()
    }

    val conf = new Configuration()
    val fs = DocsPath match {
      case url if url.startsWith("hdfs://") => FileSystem.get(new URI(url), conf)
      case _ => FileSystem.getLocal(conf).getRawFileSystem
    }
    val BuffReader = new BufferedReader(new InputStreamReader(fs.open(new Path(DocsPath+"ConnectOptions.json")), "UTF-8"))
    val js = parse(Stream.continually(BuffReader.readLine).takeWhile(_ != null).mkString)

    val User : String = getJsonValue(js, "user")
    val Password : String = getJsonValue(js, "password")
    val Address : String = getJsonValue(js, "local_address")
    //val Address : String = getJsonValue(js, "address")

    val corporate_payments = spark.read.parquet(DocsPath + "SPMshowcaseparquet/Datamarts/corporate_payments.parquet")
    val corporate_account = spark.read.parquet(DocsPath + "SPMshowcaseparquet/Datamarts/corporate_account.parquet")
    val corporate_info = spark.read.parquet(DocsPath + "SPMshowcaseparquet/Datamarts/corporate_info.parquet")
    corporate_payments.createOrReplaceTempView("corporate_payments")

    // write to db
    val ConnectionProperties = new Properties()
    ConnectionProperties.put("user", User)
    ConnectionProperties.put("password", Password)
    ConnectionProperties.put("driver", "org.postgresql.Driver")
    corporate_payments.write.mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://" + Address + "/mydb", "spmshowcase.corporate_payments", ConnectionProperties)
    corporate_account.write.mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://" + Address + "/mydb", "spmshowcase.corporate_account", ConnectionProperties)
    corporate_info.write.mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://" + Address + "/mydb", "spmshowcase.corporate_info", ConnectionProperties)
  }
}