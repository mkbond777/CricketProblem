package com.my.project.bcg

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, UserDefinedFunction}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.util.Try

/**
  * Created by DT2(M.Kumar) on 6/6/2018.
  */
object Questions extends App {

  override def main(args: Array[String]): Unit = {
    val sourceLoc = args(0)
    val sparkConf = new SparkConf().setAppName("CSV to Parquet").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val inputDf = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").load(sourceLoc)
    val indiaPlayed = inputDf.where("Team_1='India' or Team_2='India'")
    question1(indiaPlayed)
  }


  /* What is India’s total Win/Loss/Tie percentage? */

  def question1(indiaPlayed: DataFrame): (Float, Float, Float) = {
    val indiaWin = indiaPlayed.where("Winner = 'India'")
    indiaWin.cache()
    val winPercent = (indiaWin.count.toFloat / indiaPlayed.count.toFloat) * 100
    val noResult = indiaPlayed.where("Winner = 'no result' or Winner = 'tied'")
    noResult.cache()
    val noResultPercent = (noResult.count.toFloat / indiaPlayed.count.toFloat) * 100
    //    val indiaLoss = indiaPlayed.where("Winner != 'India' and Winner != 'no result' and Winner != 'tied'")
    //    val lossPercent = (indiaLoss.count.toFloat/indiaPlayed.count.toFloat) * 100
    val lossPercent = 100 - (winPercent + noResultPercent)
    (winPercent, noResultPercent, lossPercent)
  }

  /* What is India’s Win/Loss/Tie percentage in away and home matches? */

  def question2(indiaPlayed: DataFrame): List[(Float, Float, Float)] = {
    val indiaHome = indiaPlayed.where("Host_Country = 'India'")
    val indiaAway = indiaPlayed.where("Host_Country != 'India'")
    val resultsHome = question1(indiaHome)
    val resultsAway = question1(indiaAway)
    List(resultsAway, resultsHome)
  }

  /* How many matches has India played against different ICC teams? */

  def question3(df: DataFrame): Map[String, Long] = {
    val matchAgaianstIndiaCount = teamList(df).map(x => {
      val team = x.toString
      val oppAsTeam2 = df.where(s"Team_1 = 'India' and Team_2 = '$team'").count()
      val oppAsTeam1 = df.where(s"Team_1 = '$team' and Team_2 = 'India'").count()
      (team, oppAsTeam1 + oppAsTeam2)
    })
    matchAgaianstIndiaCount.toMap
  }

  /* How many matches India has won or lost against different teams? */

  def question4(df: DataFrame): Set[(String, Long, Long)] = {

    def winLoss(df: DataFrame, oppTeam: String): (Long, Long) =
      (df.where("Winner = 'India'").count,
        df.where(s"Winner = '$oppTeam'").count)

    val winLossAgaianstIndiaCount = teamList(df).map(x => {
      val team = x.toString
      val oppAsTeam2 = df.where(s"Team_1 = 'India' and Team_2 = '$team'")
      val (winOppTeam2, lossOppTeam2) = winLoss(oppAsTeam2, team)
      val oppAsTeam1 = df.where(s"Team_1 = '$team' and Team_2 = 'India'")
      val (winOppTeam1, lossOppTeam1) = winLoss(oppAsTeam1, team)
      (team, winOppTeam1 + winOppTeam2, lossOppTeam1 + lossOppTeam2)
    })
    winLossAgaianstIndiaCount
  }

  /* Which are the home and away grounds where India has played most number of matches? */
  def question5(df: DataFrame, sqlContext: SQLContext): Unit = {
    val indiaMatches = df.where("Team_1 = 'India' or Team_2 = 'India'")
    indiaMatches.cache()
    //    val homeGround = indiaMatches.where("Host_Country = 'India'").select("Host_Country","Ground")
    //      .groupBy("Ground").count()
    //
    //    val w = Window.partitionBy("Ground").orderBy(col("count").desc)
    //    homeGround.withColumn("rank",row_number().over(w)).where("rank = 1").select("Ground")

    //Ignores Duplicate
    //    import sqlContext.implicits._
    //
    //    val homeGround = indiaMatches.where("Host_Country = 'India'")
    //      .groupBy(col("Ground")).count()
    //      .as[(String,Long)]
    //      .reduce((x,y) => if (x._2 > y._2) x else y )

    val homeGround = indiaMatches.where("Host_Country = 'India'")
      .groupBy(col("Ground")).count()
    val maxValue = homeGround.agg(max(col("count")).alias("max_count"))
    val homeMax = homeGround.join(broadcast(maxValue),col("count") === col("max_count"))
      .drop("max_count")

    val awayGround = indiaMatches.where("Host_Country != 'India'")
      .groupBy(col("Ground")).count()
    val maxCount = awayGround.agg(max(col("count")).alias("max_count"))
    val awayMax = awayGround.join(broadcast(maxCount),col("count") === col("max_count"))
      .drop("max_count")

    homeMax.show()
    awayMax.show()

  }

  /* What has been the average Indian win or loss by Runs per year? */

  def question6 (df : DataFrame) : Unit = {
    val indiaMatches = df.where("Team_1 = 'India' or Team_2 = 'India'")
    indiaMatches.cache()
    val get_last: UserDefinedFunction = udf((xs: Seq[String]) => Try(xs.last).toOption)

    val properYear = udf((xs : String) => {
      if (xs.toInt < 100)
        if (xs.toInt > 20)
          xs.toInt + 1900
        else
          xs.toInt + 2000
      else
        xs.toInt
    })

    val runsColumn = udf((xs : String) => if (xs.contains("runs") || xs.contains("run")) Some(xs) else None)

    val dfWithYear = indiaMatches
      .withColumn("Year",get_last(split(col("Match_Date"),"-| ")))
    val dfWithYearProper = dfWithYear.withColumn("year_new", properYear(col("Year")))
    val dfWithRunMargin = dfWithYearProper
      .withColumn("run_margin",runsColumn(col("Margin")))
      .where("run_margin != 'null'")
    val dfWithRuns = dfWithRunMargin
      .withColumn("runs",split(col("run_margin")," ").getItem(0).cast(IntegerType))
    dfWithRuns.cache()
    val indiaWinner = dfWithRuns.where("Winner = 'India'")
    val avgRunWinPerYear = indiaWinner.groupBy("year_new").avg("runs")
    val oppositionWinner = dfWithRuns.where("Winner != 'India'")
    val lossAvgRunsPerYear = oppositionWinner.groupBy("year_new").avg("runs")
    print("Hello")

  }

  def teamList(df: DataFrame): Set[Any] = {
    df.cache()
    val diffTeam1 = df.where("Team_1 != 'India'").select("Team_1")
      .distinct().map(r => r(0)).collect().toSet
    val diffTeam2 = df.where("Team_2 != 'India'").select("Team_2")
      .distinct().map(r => r(0)).collect().toSet
    if (diffTeam1.size.equals(diffTeam2.size)) diffTeam1 else diffTeam1.union(diffTeam2)
  }

}
