import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object SPMshowcaseAPI extends App {
  override def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder()
      //.master("local[4]")
      .appName("SPMshowcase")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    var DocsPath = "C:\\Users\\apolivanov\\stajirovka\\apolivanov\\projects\\SPMshowcase\\docs\\"
    //var DocsPath = "./docs/"
    if (args.length >= 1) {
      DocsPath = args(0)
    }
    else {
      log.warn("Docs path should be given as first argument")
      //sys.exit()
    }

    import spark.implicits._
    val mask1 = spark.sparkContext.textFile(DocsPath+"Masks/mask1").collect().mkString
      .replace("\\", "\\\\")
      .replace("%","")
      .split(",").toSeq.toDF().rdd.map(r => r(0)).collect().mkString("|")
    val mask2 = spark.sparkContext.textFile(DocsPath+"Masks/mask2").collect().mkString
      .replace("\\", "\\\\")
      .replace("%","")
      .split(",").toSeq.toDF().rdd.map(r => r(0)).collect().mkString("|")

    val begin = System.nanoTime()

    val Client = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscClients.parquet")
    val Account = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscAccounts.parquet")
    val Operation = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscOps.parquet")
    val Rate = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscCourses.parquet")

    Client.repartition(Client("RegisterDate"))
    Account.repartition(Account("DateOpen"))
    Operation.repartition(Operation("DateOp"))
    Rate.repartition(Rate("RateDate"))

    val WindowSpec = Window.partitionBy("AccountDB", "AccountCR", "Amount", "Comment")
      .orderBy(desc("RateDate"))

    val cteOperation = Rate
      .join(Operation, (Operation("DateOp") >= Rate("RateDate")) and (Operation("Currency") === Rate("Currency")), "inner")
      .where(col("DateOp").between(to_date(lit("2020-11-01")), to_date(lit("2020-11-05"))))
      .withColumn("rank", rank().over(WindowSpec))
      .filter(col("rank") === 1)
      .select(col("AccountDB"), col("AccountCR"), col("Comment"),
        (col("Amount")*col("Rate")).as("Amount"), col("DateOp"))
      .cache()

    val cteAccountClient = Account
      .join(Client, Client("ClientId") === Account("ClientId"), "inner")
      .select(
        Account("AccountId"), Account("ClientID").as("cteAccountClientID"), Account("AccountNum"), Account("DateOpen"),
        Client("ClientName"), Client("Type"), Client("Form"), Client("RegisterDate"))
      .cache()

    val cteDB = cteAccountClient
      .join(cteOperation, cteAccountClient("AccountId") === cteOperation("AccountDB"), "inner")
      .select(
        cteAccountClient("AccountId"), cteAccountClient("cteAccountClientID").as("cteDBClientID"),
        cteAccountClient("AccountNum"), cteAccountClient("DateOpen"),
        cteOperation("Amount"), cteOperation("DateOp"), cteOperation("Comment"), cteOperation("AccountCR"),
        cteAccountClient("ClientName"), cteAccountClient("Type"), cteAccountClient("Form"), cteAccountClient("RegisterDate"))
      .cache()

    val cteCR = cteAccountClient
      .join(cteOperation, cteAccountClient("AccountId") === cteOperation("AccountCR"), "inner")
      .select(
        cteAccountClient("AccountId"), cteAccountClient("cteAccountClientID").as("cteCRClientID"),
        cteAccountClient("AccountNum"), cteAccountClient("DateOpen"),
        cteOperation("Amount"), cteOperation("DateOp"), cteOperation("Comment"), cteOperation("AccountDB"),
        cteAccountClient("ClientName"), cteAccountClient("Type"), cteAccountClient("Form"), cteAccountClient("RegisterDate"))
      .cache()

    val PaymentAmt = cteDB
      .groupBy(cteDB("AccountId").as("PaymentAmtAccountId"), cteDB("DateOp").as("PaymentAmtDateOp"), cteDB("cteDBClientID"))
      .agg(sum(cteDB("Amount")).cast(DecimalType(11, 2)).as("PaymentAmt"))
      .cache()

    val EnrollementAmt = cteCR
      .groupBy(cteCR("AccountId").as("EnrollementAmtAccountId"), cteCR("DateOp").as("EnrollementAmtDateOp"), cteCR("cteCRClientID"))
      .agg(sum(cteCR("Amount")).cast(DecimalType(11, 2)).as("EnrollementAmt"))
      .cache()

    val ctePE = PaymentAmt
      .join(EnrollementAmt, (PaymentAmt("PaymentAmtAccountId") === EnrollementAmt("EnrollementAmtAccountId")) and
        (PaymentAmt("PaymentAmtDateOp") === EnrollementAmt("EnrollementAmtDateOp")), "full")
      .withColumn("AccountId", when(EnrollementAmt("EnrollementAmtAccountId").isNull, PaymentAmt("PaymentAmtAccountId"))
        .when(PaymentAmt("PaymentAmtAccountId").isNull, EnrollementAmt("EnrollementAmtAccountId")))
      .withColumn("CutoffDt", when(EnrollementAmt("EnrollementAmtDateOp").isNull, PaymentAmt("PaymentAmtDateOp"))
        .when(PaymentAmt("PaymentAmtDateOp").isNull, EnrollementAmt("EnrollementAmtDateOp")))
      .withColumn("ClientID", when(EnrollementAmt("cteCRClientID").isNull, PaymentAmt("cteDBClientID"))
        .when(PaymentAmt("cteDBClientID").isNull, EnrollementAmt("cteCRClientID")))
      .select(PaymentAmt("PaymentAmt"), EnrollementAmt("EnrollementAmt"), col("AccountId"),
        col("CutoffDt"), col("ClientID"))
      .cache()

    val ClearAmt = cteDB
      .where(col("AccountNum").like("40802%"))
      .groupBy(cteDB("AccountCR").as("ClearAmtAccountId"), cteDB("DateOp").as("ClearAmtDateOp"))
      .agg(sum(cteDB("Amount")).cast(DecimalType(11, 2)).as("ClearAmt"))
      .cache()

    val TaxAmt = cteCR
      .where(col("AccountNum").like("40702%"))
      .groupBy(cteCR("AccountDB").as("TaxAmtAccountId"), cteCR("DateOp").as("TaxAmtDateOp"))
      .agg(sum(cteCR("Amount")).cast(DecimalType(11, 2)).as("TaxAmt"))
      .cache()

    val FLAmt = cteDB
      .where(cteDB("Type") === "Ф")
      .groupBy(cteDB("AccountId").as("FLAmtAccountId"), cteDB("DateOp").as("FLAmtDateOp"))
      .agg(sum(cteDB("Amount")).cast(DecimalType(11, 2)).as("FLAmt"))
      .cache()

    val CarsAmt = cteDB.where(!cteDB("Comment").rlike(mask1))
      .groupBy(cteDB("AccountId").as("CarsAmtAccountId"), cteDB("DateOp").as("CarsAmtDateOp"))
      .agg(sum(cteDB("Amount")).cast(DecimalType(11, 2)).as("CarsAmt"))
      .cache()

    val FoodAmt = cteCR.where(cteCR("Comment").rlike(mask2))
      .groupBy(cteCR("AccountId").as("FoodAmtAccountId"), cteCR("DateOp").as("FoodAmtDateOp"))
      .agg(sum(cteCR("Amount")).cast(DecimalType(11, 2)).as("FoodAmt"))
      .cache()

    val tempDF = ctePE
      .join(TaxAmt, (ctePE("AccountId") === TaxAmt("TaxAmtAccountId")) && (ctePE("CutoffDt") === TaxAmt("TaxAmtDateOp")), "left")
      .join(ClearAmt, (ctePE("AccountId") === ClearAmt("ClearAmtAccountId")) && (ctePE("CutoffDt") === ClearAmt("ClearAmtDateOp")), "left")
      .join(FLAmt, (ctePE("AccountId") === FLAmt("FLAmtAccountId")).and(ctePE("CutoffDt") === FLAmt("FLAmtDateOp")), "left")
      .join(CarsAmt, (ctePE("AccountId") === CarsAmt("CarsAmtAccountId")) && (ctePE("CutoffDt") === CarsAmt("CarsAmtDateOp")), "left")
      .join(FoodAmt, (ctePE("AccountId") === FoodAmt("FoodAmtAccountId")) && (ctePE("CutoffDt") === FoodAmt("FoodAmtDateOp")), "left")
      .join(cteAccountClient, ctePE("AccountId") === cteAccountClient("AccountId"), "left")
      .select(
        ctePE("AccountId"), ctePE("PaymentAmt"), ctePE("EnrollementAmt"), ctePE("CutoffDt"),
        TaxAmt("TaxAmt"), ClearAmt("ClearAmt"), CarsAmt("CarsAmt"), FoodAmt("FoodAmt"), FLAmt("FLAmt"),
        cteAccountClient("ClientName"), cteAccountClient("Type"),
        cteAccountClient("Form"), cteAccountClient("RegisterDate"), cteAccountClient("AccountNum"),
        cteAccountClient("DateOpen"), cteAccountClient("cteAccountClientID").as("ClientID"))
      .na.fill(0)
      .cache()

    val corporate_payments = tempDF
      .select(
        tempDF("AccountId"),
        tempDF("ClientID"),
        tempDF("PaymentAmt"),
        tempDF("EnrollementAmt"),
        tempDF("TaxAmt"),
        tempDF("ClearAmt"),
        tempDF("CarsAmt"),
        tempDF("FoodAmt"),
        tempDF("FLAmt"),
        tempDF("CutoffDt")
      ).orderBy("AccountId").cache()

    val corporate_account = tempDF
      .select(
        tempDF("AccountId"),
        tempDF("AccountNum"),
        tempDF("DateOpen"),
        tempDF("ClientID"),
        tempDF("ClientName"),
        (tempDF("PaymentAmt") + tempDF("EnrollementAmt")).as("TotalAmt"),
        tempDF("CutoffDt")
      ).cache()

    val corporate_info = tempDF
      .select(
        tempDF("ClientID"),
        tempDF("ClientName"),
        tempDF("Type"),
        tempDF("Form"),
        tempDF("RegisterDate"),
        (tempDF("PaymentAmt") + tempDF("EnrollementAmt")).as("TotalAmt"),
        tempDF("CutoffDt")
      ).groupBy("ClientID", "CutoffDt", "RegisterDate", "Form", "Type", "ClientName").agg(sum("TotalAmt").as("TotalAmt"))
      .select(
        ("ClientID"),
        ("ClientName"),
        ("Type"),
        ("Form"),
        ("RegisterDate"),
        ("TotalAmt"),
        ("CutoffDt")
      ).cache()

    spark.time(corporate_payments.show())
    spark.time(corporate_account.show())
    spark.time(corporate_info.show())

    //corporate_payments.orderBy(col("AccountId").asc).show()
    //corporate_account.orderBy(col("AccountId").asc).show()
    //corporate_info.orderBy(col("ClientID").asc).show()

    // save to parquet
    var DatamartsPath = "./"
    if (args.length >= 2)
    {
      DatamartsPath = args(1) + "SPMshowcaseparquet/Datamarts/"
    }
    else
    {
      log.warn("No datamarts path given")
      DatamartsPath = DocsPath + "SPMshowcaseparquet/Datamarts/"
    }

    corporate_payments.write.mode(SaveMode.Overwrite)
      .partitionBy("CutoffDt")
      .parquet(DatamartsPath + "corporate_payments.parquet")

    corporate_account.write.mode(SaveMode.Overwrite)
      .partitionBy("CutoffDt")
      .parquet(DatamartsPath + "corporate_account.parquet")

    corporate_info.write.mode(SaveMode.Overwrite)
      .partitionBy("CutoffDt")
      .parquet(DatamartsPath + "corporate_info.parquet")

    val end = System.nanoTime()
    println("Overall Time: " + ((end - begin) / 1000000000) + " sec")

  }
}