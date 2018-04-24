package com.sposter.rdd

import org.apache.spark.SparkContext

/**
  * 本质上来讲，reduceByKey函数（说算子也可以）只作用于包含key-value的RDDS上，它是
transformation类型的算子，这也就意味着它是懒加载的（就是说不调用Action的方法，是不会去计算的
）,在使用时，我们需要传递一个相关的函数作为参数，这个函数将会被应用到源RDD上并且创建一个新的
RDD作为返回结果，这个算子作为data Shuffling 在分区的时候被广泛使用】
  */
object ReduceByKey {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "ReduceByKeyToDriver Test")
    val data1 = Array[(String, Int)](("K", 1), ("U", 2),
      ("U", 3), ("W", 4),
      ("W", 5), ("W", 6))
    val pairs = sc.parallelize(data1, 3)
    //val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    //val result = pairs.fold(("K0",10))((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    val result = pairs.reduceByKey(_ + _, 2)
    result.foreach(println)
  }

}