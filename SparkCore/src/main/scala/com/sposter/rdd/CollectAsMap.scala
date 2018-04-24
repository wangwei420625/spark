package com.sposter.rdd

import org.apache.spark.SparkContext

/**
  * 如果RDD中同一个Key中存在多个Value，那么后面的Value将会把前面的Value覆盖，
  * 最终得到的结果就是Key唯一，而且对应一个Value。
  */
object CollectAsMap {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "CollectAsMap Test")
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6))

    // as same as "val pairs = sc.parallelize(data, 3)"
    val pairs = sc.makeRDD(data, 3)

    val result = pairs.collectAsMap

    // output Map(A -> 1, C -> 6, B -> 3)
    print(result)
  }


}
