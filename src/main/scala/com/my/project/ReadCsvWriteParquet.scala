package com.my.project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by DT2(M.Kumar) on 6/6/2018.
  */
object ReadCsvWriteParquet extends App {

  override def main(args: Array[String]): Unit = {
    val sourceLoc = args(0)
    val destLoc = args(1)

    val sparkConf = new SparkConf().setAppName("CSV to Parquet").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val readDf = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true").load(sourceLoc)
    //readDf.withColumnRenamed("Team 1","Team_1")
    readDf.write.format("parquet").save(destLoc)
  }

}
