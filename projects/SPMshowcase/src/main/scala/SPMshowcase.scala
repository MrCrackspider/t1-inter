import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, LogManager}

import scala.io.Source
import org.apache.spark.sql.functions.typedLit

//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path

object SPMshowcase extends App {
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
    if (args.length >= 1)
      {
        DocsPath = args(0)
      }
    else
      {
        log.warn("Docs path should be given as first argument")
        //sys.exit()
      }

    val begin = System.nanoTime()

    val Client = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscClients.parquet")
    val Account = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscAccounts.parquet")
    val Operation = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscOps.parquet")
    val Rate = spark.read.parquet(DocsPath + "SPMshowcaseparquet/SPMscCourses.parquet")

    Client.repartition(Client("RegisterDate"))
    Account.repartition(Account("DateOpen"))
    Operation.repartition(Operation("DateOp"))
    Rate.repartition(Rate("RateDate"))

    Client.createOrReplaceTempView("Client")
    Account.createOrReplaceTempView("Account")
    Operation.createOrReplaceTempView("Operation")
    Rate.createOrReplaceTempView("Rate")

    val calculation_params_tech = spark.read.options(Map("header" -> "true", "delimiter" -> ";"))
      .csv(DocsPath + "techtable.csv")
    calculation_params_tech.createOrReplaceTempView("ParamsTech")

/*    import spark.implicits._

    var src : Source = Source.fromFile(DocsPath + "Masks/mask1", enc = "cp1251")
    val str1 : Seq[String] = src.mkString.replace("\\", "\\\\").split(",")
    src.close()
    val mask1 = str1.toDF("Value").withColumn("ParamName",  typedLit("list1"))

    src = Source.fromFile(DocsPath + "Masks/mask2", enc = "cp1251")
    val str2 : Seq[String] = src.mkString.replace("\\", "\\\\").split(",")
    src.close()
    val mask2 = str2.toDF("Value").withColumn("ParamName",  typedLit("list2"))
    mask1.union(mask2).createOrReplaceTempView("ParamsTech")*/

    val tempDF = spark.sql("""with

    cteOperation as
    (select c1.* from
    (select Operation.AccountDB, Operation.AccountCR, Operation.DateOp, Operation.Comment, Operation.Amount * Rate.Rate as Amount,
    rank() over (partition by AccountDB, AccountCR, Amount, Comment order by RateDate desc) as rank from Operation
    join Rate on Operation.DateOp >= Rate.RateDate and Operation.Currency = Rate.Currency
    where Operation.DateOp between '2020-11-01' and '2020-11-05')c1 where rank = 1),

    cteAccountClient as
    (select Account.AccountId, Account.AccountNum, Account.DateOpen,
    Client.ClientID, Client.Type, Client.Form, Client.ClientName, Client.RegisterDate from Account
    join Client on Account.ClientID = Client.ClientID),

    cteDB as
    (select
    cteAccountClient.AccountId, cteAccountClient.AccountNum, cteAccountClient.ClientID,
    cteOperation.DateOp, cteOperation.Comment, cteOperation.Amount from cteAccountClient
    join cteOperation on cteAccountClient.AccountId = cteOperation.AccountDB),

    cteCR as
    (select
    cteAccountClient.AccountId, cteAccountClient.AccountNum, cteAccountClient.ClientID, cteAccountClient.Type,
    cteOperation.DateOp, cteOperation.Comment, cteOperation.Amount from cteAccountClient
    join cteOperation on cteAccountClient.AccountId = cteOperation.AccountCR),

    ctePE as
    (select
    case when t1.AccountId is null then t2.AccountId else t1.AccountId end as AccountId,
    case when t1.ClientID is null then t2.ClientID else t1.ClientID end as ClientID,
    case when t1.DateOp is null then t2.DateOp else t1.DateOp end as CutoffDt,
    case when t1.PaymentAmt is null then 0 else t1.PaymentAmt end as PaymentAmt,
    case when t2.EnrollementAmt is null then 0 else t2.EnrollementAmt end as EnrollementAmt
    from
    (Select cteDB.AccountId, cteDB.ClientID,
    cteDB.DateOp, cast(sum(cteDB.Amount) as decimal(9,2)) as PaymentAmt
    from cteDB
    group by cteDB.AccountId, cteDB.ClientID, cteDB.DateOp)t1
    full join
    (select cteCR.AccountId, cteCR.ClientID,
    cteCR.DateOp, cast(sum(cteCR.Amount) as decimal(9,2)) as EnrollementAmt
    from cteCR
    group by cteCR.AccountId, cteCR.ClientID, cteCR.DateOp)t2
    on t1.AccountId = t2.AccountId and t1.DateOp = t2.DateOp)

    select ctePE.*,
    case when t3.TaxAmt is null then 0 else t3.TaxAmt end as TaxAmt,
    case when t4.ClearAmt is null then 0 else t4.ClearAmt end as ClearAmt,
    case when t5.FLAmt is null then 0 else t5.FLAmt end as FLAmt,
    case when t6.CarsAmt is null then 0 else t6.CarsAmt end as CarsAmt,
    case when t7.FoodAmt is null then 0 else t7.FoodAmt end as FoodAmt,
    ctePE.PaymentAmt + ctePE.EnrollementAmt as TotalAmt,
    cteAccountClient.ClientName, cteAccountClient.Type, cteAccountClient.Form,
    cteAccountClient.RegisterDate, cteAccountClient.DateOpen, cteAccountClient.AccountNum

    from ctePE

    left join cteAccountClient on ctePE.AccountId = cteAccountClient.AccountId

    left join

    (Select cteDB.AccountId, cast(sum(cteDB.Amount) as decimal(9,2)) as TaxAmt, cteDB.DateOp from cteDB
    where AccountNum like '40702%'
    group by cteDB.AccountId, cteDB.DateOp)t3
    on (ctePE.AccountId = t3.AccountId) and (ctePE.CutoffDt = t3.DateOp)

    left join

    (Select cteCR.AccountId, cast(sum(cteCR.Amount) as decimal(9,2)) as ClearAmt, cteCR.DateOp from cteCR
    where AccountNum like '40802%'
    group by cteCR.AccountId, cteCR.DateOp)t4
    on (ctePE.AccountId = t4.AccountId) and (ctePE.CutoffDt = t4.DateOp)

    left join

    (select cteCR.AccountId, cast(sum(cteCR.Amount) as decimal(9,2)) as FLAmt, cteCR.DateOp from cteCR
    where cteCR.Type = 'Ф'
    group by cteCR.AccountId, cteCR.DateOp)t5
    on (ctePE.AccountId = t5.AccountId) and (ctePE.CutoffDt = t5.DateOp)

    left join

    (select cteDB.AccountId, cast(sum(cteDB.Amount) as decimal(9,2)) as CarsAmt, cteDB.DateOp
    from cteDB
    where not exists (select value from ParamsTech where ParamName = 'list1' and cteDB.Comment like ParamsTech.Value)
    group by cteDB.AccountId, cteDB.DateOp)t6
    on (ctePE.AccountId = t6.AccountId) and (ctePE.CutoffDt = t6.DateOp)

    left join

    (select cteCR.AccountId, cast(sum(cteCR.Amount) as decimal(9,2)) as FoodAmt, cteCR.DateOp
    from cteCR
    where exists (select value from ParamsTech where ParamName = 'list2' and cteCR.Comment like ParamsTech.Value)
    group by cteCR.AccountId, cteCR.DateOp)t7
    on (ctePE.AccountId = t7.AccountId) and (ctePE.CutoffDt = t7.DateOp)

    --order by ctePE.AccountId

    """).persist()

    tempDF.createOrReplaceTempView("tempDF")

    // --- corporate_payments ---
    val corporate_payments = spark.sql("""
    select
    tempDF.AccountId,
    tempDF.ClientId,
    tempDF.PaymentAmt,
    tempDF.EnrollementAmt,
    tempDF.TaxAmt,
    tempDF.ClearAmt,
    tempDF.CarsAmt,
    tempDF.FoodAmt,
    tempDF.FLAmt,
    tempDF.CutoffDt
    from tempDF
    """).persist()

    // --- corporate_account ---
    val corporate_account = spark.sql("""
    select
    tempDF.AccountId,
    tempDF.AccountNum,
    tempDF.DateOpen,
    tempDF.ClientID,
    tempDF.ClientName,
    tempDF.TotalAmt,
    tempDF.CutoffDt from tempDF
    """).persist()

    // --- corporate_info ---
    val corporate_info = spark.sql("""
    select
    tempDF.ClientID,
    tempDF.ClientName,
    tempDF.Type,
    tempDF.Form,
    tempDF.RegisterDate,
    sum(tempDF.TotalAmt) as TotalAmt,
    tempDF.CutoffDt from tempDF
    group by tempDF.ClientID, tempDF.CutoffDt, tempDF.ClientName, tempDF.Type, tempDF.Form, tempDF.RegisterDate
    """).persist()

    spark.time(corporate_payments.show())
    spark.time(corporate_account.show())
    spark.time(corporate_info.show)

    // save to parquet
    var DatamartsPath = ""
    if (args.length >= 2)
      {
        DatamartsPath = args(1) + "SPMshowcaseparquet/Datamarts/"
      }
    else
      {
        log.warn("No datamarts path given. Using docs path as default")
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