package com.my.project

import com.my.project.bcg.Questions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

/**
  * Created by DT2(M.Kumar) on 6/6/2018.
  */
class ReadCsvWriteParquetTest extends FunSuite {

  test("testMain") {

    val sourceLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/originalDataset.csv"
    val destLoc = "file:///C:/Users/m.kumar/manish/Personal/Interview/BCG/parquetfile"
    val output: Unit = ReadCsvWriteParquet.main(Array(sourceLoc, destLoc))
    assert(output == Unit)

  }

}
