package com.sposter.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
  * Lookup函数对（Key，Value）型的RDD操作，返回指定Key对应的元素形成的Seq。
  * 这个函数处理优化的部分在于，如果这个RDD包含分区器，则只会对应处理K所在的分区，
  * 然后返回由（K，V）形成的Seq。 如果RDD不包含分区器，则需要对全RDD元素进行暴力扫描处理，
  * 搜索指定K对应的元素。
  */
object Lookup {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "LookUp Test")

    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6))

    val pairs = sc.parallelize(data, 3)

    val finalRDD = pairs.lookup("B")

    finalRDD.foreach(println)
    // output:
    // 2
    // 3
  }
}