package com.sposter.wordcount

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHiveSpark {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ReadHiveSpark")
      .master("local[*]")
      .getOrCreate()


    //用于隐式转换，如将RDD转换为DataFrames
    import  spark.implicits._

    val result: DataFrame = spark.sql("SELECT * FROM student")

    result.show()







  }

}
