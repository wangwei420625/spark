package com.sposter.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class LambdaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LambdaWordCount").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("D:\\workspace\\workspace02\\Spark\\SparkCore\\src\\test.txt");
        JavaRDD<String> words = lines.flatMap(line ->
                Arrays.asList(line.split(" ")).iterator()
        );

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word ->
                new Tuple2<>(word, 1)
        );

        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());

        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());

        List<Tuple2<String, Integer>> collect = reduced.collect();

        System.out.println(collect);



    }
}
