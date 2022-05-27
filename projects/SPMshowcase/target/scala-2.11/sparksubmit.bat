@echo off
echo Spark submit start
spark-submit --master yarn ^
--deploy-mode cluster --executor-memory 2g --executor-cores 2 ^
--class SPMshowcase ^
SPMshowcase.jar ^
hdfs://172.18.0.2:8020/tmp/Docs/

:: hdfs docs path:
:: hdfs://172.18.0.2:8020/tmp/
:: C:\Users\apolivanov\stajirovka\apolivanov\projects\SPMshowcase\docs\
:: spark://192.168.56.1:7077
:: C:\Users\apolivanov\stajirovka\apolivanov\projects\SPMshowcase\target\scala-2.11\SPMshowcase.jar
:: classes: SPMshowcase / CsvToParquet / SPMshowcaseAPI / ParquetToPostgre
:: spark-submit --master local[4] --deploy-mode client --class SPMshowcase --driver-memory 2g --executor-memory 2g --executor-cores 2 /tmp/SPMshowcase.jar hdfs://172.18.0.2:8020/tmp/Docs/
:: spark-submit --master yarn --deploy-mode cluster --class SPMshowcase --driver-memory 2g --executor-memory 2g --executor-cores 2 /tmp/SPMshowcase.jar hdfs://172.18.0.2:8020/tmp/Docs/
:: hdfs dfs -copyToLocal hdfs://172.18.0.2:8020/tmp/SPMshowcase.jar /tmp/Docs/
