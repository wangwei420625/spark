package com.sposter.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Aggregate {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "AggregateAction Test")
    val data = Array[(String, Int)](("A1", 1), ("A2", 2),
      ("B1", 3), ("B2", 4),
      ("C1", 5), ("C2", 6))

    val pairs: RDD[(String, Int)] = sc.parallelize(data, 3)

    // output:
    // 	(A1,1)(A2,2)
    //  (B1,3)(B2,4)
    //	(C1,5)(C2,6)
    pairs.foreach(print)

    val result = pairs.aggregate(("", 0))((U, T) => (U._1 + T._1, U._2 + T._2), (U, T) =>
      ("[" + U._1 + T._1 + "] ", U._2 + T._2))

    // output ([[[A1A2] B1B2] C1C2] ,21)
    println(result)
  }

}
