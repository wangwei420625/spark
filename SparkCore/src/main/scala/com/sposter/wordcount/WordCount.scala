package com.sposter.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val value: RDD[(String, Int)] = sc.textFile("D:\\workspace\\workspace02\\Spark\\SparkCore\\src\\test.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    value.collect().foreach(println)
    sc.stop()


  }

}
