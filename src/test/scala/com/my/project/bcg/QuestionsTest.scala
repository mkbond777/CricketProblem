package com.my.project.bcg

import com.my.project.ReadCsvWriteParquet
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
  * Created by DT2(M.Kumar) on 6/6/2018.
  */
class QuestionsTest extends FunSuite {

  test("testMain") {

    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/ContinousDataset.csv"
    val output: Unit = Questions.main(Array(sourceLoc))
    true
  }

  test("question1") {
    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/originalDataset.csv"
    val df = indiaPlayedDf(sourceLoc)
    val q1Result = Questions.question1(df)
    assert((q1Result._1 + q1Result._2 + q1Result._3) == 100)
  }

  test("question2") {

    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/ContinousDataset.csv"
    val df = indiaPlayedDf(sourceLoc).where("Unnamed <= 3747")
    val q2Result = Questions.question2(df)
    assert(q2Result.map(l => l._1 + l._2 + l._3).sum == 200)

  }

  test("question3") {

    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/originalDataset.csv"
    val inputDf = getCsvReader.load(sourceLoc)
    val q2Result = Questions.question3(inputDf)
    print("Hello")

  }

  test("question4") {

    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/originalDataset.csv"
    val inputDf = getCsvReader.load(sourceLoc)
    val q4Result = Questions.question4(inputDf)
    print("Hello")

  }

  test("question5") {

    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/ContinousDataset.csv"
    val sqlContext = getSqlContext
    val inputDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .load(sourceLoc).where("Unnamed <= 3747")
    val q4Result = Questions.question5(inputDf,sqlContext)

  }

  test ("Question6") {
    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/originalDataset.csv"
    val sqlContext = getSqlContext
    val inputDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema","true").load(sourceLoc)
    val q6Result = Questions.question6(inputDf)
    print("Hello")
  }

  def indiaPlayedDf (loc : String) : DataFrame = {
    val inputDf = getCsvReader.load(loc)
    inputDf.where("Team_1='India' or Team_2='India'")
  }

  def getCsvReader : DataFrameReader = {
    val sqlContext = getSqlContext
    sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
  }

  def getSqlContext : SQLContext = {
    val sparkConf = new SparkConf().setAppName("CSV to Parquet").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    sqlContext
  }

}
