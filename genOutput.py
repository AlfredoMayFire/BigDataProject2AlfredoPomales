from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
import csv

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionInstance' not in globals()):
        globals()['sparkSessionInstance'] = SparkSession.builder.config(conf=sparkConf) \
                                            .enableHiveSupport().getOrCreate()
    return globals()['sparkSessionInstance']

def genOutput():
    spark = getSparkSessionInstance(sc.getConf())
    df = spark.sql("use default")

    #Exercise 4a
    #df = spark.sql("select hashtag, sum(total) as count from hashtagTable where timestamp between cast('2018-12-06 12:00:00' as timestamp) and cast('2018-12-06 13:00:00' as timestamp) group by hashtag order by total desc limit 10")
    #Exercise 4b
    #df = spark.sql("select word, sum(total) as count from keywordTable where timestamp between cast('2018-12-06 12:00:00' as timestamp) and cast('2018-12-06 13:00:00' as timestamp) group by word order by total desc limit 10")
    #Exercise 4c
    #df = spark.sql("select name, sum(total) as count from snTable where timestamp between cast('2018-12-06 12:00:00' as timestamp)- INTERVAL 12 HOUR and cast('2018-12-06 12:00:00' as timestamp) group by name order by total desc limit 10")
    #Exercise 5 Day 1
    #df = spark.sql("select keyword,sum(total) as count from ocurrencesTable where timestamp between cast('2018-12-06 13:00:00' as timestamp) and cast('2018-12-06 12:00:00' as timestamp) group by keyword order by total desc limit 10")
    #Exercise 5 Day 2
    #df = spark.sql("select keyword,sum(total) as count from ocurrencesTable where timestamp between cast('2018-12-07 22:00:00' as timestamp) and cast('2018-12-07 21:00:00' as timestamp) group by keyword order by total desc limit 10")
    #Exercise 5 Day 3

    df = spark.sql("select keyword,sum(total) as count from ocurrencesTable where timestamp between cast('2018-12-08 1:00:00' as timestamp) and cast('2018-12-08 00:00:00' as timestamp) group by keyword order by total desc limit 10")
    df.show()

    #Pon archivo de salida, no puede existir
    df.repartition(1).write.csv("/data/P2Ejercicio5Day3Alfredo.csv")

if __name__ == "__main__":
    sc = SparkContext(appName="Save as CSV")
    genOutput()
