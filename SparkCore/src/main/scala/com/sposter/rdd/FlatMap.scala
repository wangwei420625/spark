package com.sposter.rdd

import org.apache.spark.SparkContext
//将原来 RDD 中的每个元素通过函数 f 转换为新的元素，并将生成的 RDD 的每个集合中的元素合并为一个集合，
//内部创建 FlatMappedRDD(this，sc.clean(f))。
object FlatMap {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "FlatMap Test")
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6)
    )


    val pairs = sc.makeRDD(data, 3)


    val result = pairs.flatMap(T => (T._1 + T._2))

    result.foreach(println)
  }

}
