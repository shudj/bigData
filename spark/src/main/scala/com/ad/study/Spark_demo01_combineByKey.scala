package com.ad.study

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * @author: shudj
  * @time: 2019/11/18 15:42
  * @description:
  *
  * def combineByKey[C](createCombiner: (V) => C,
  * mergeValue: (C, V) => C,
  * mergeCombiners: (C, C) => C): RD
  *
  * createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就 
  * 和之前的某个元素的键相同。如果这是一个新的元素， combineByKey() 会使用一个叫作 createCombiner() 的函数来创建 
  * 那个键对应的累加器的初始值
  * mergeValue: 如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 方法将该键的累加器对应的当前值与这个新的值进行合并
  * mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更 
  * 多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各 
  * 个分区的结果进行合并。
  */
object Spark_demo01_combineByKey {

    val scores = List(
        ScoreDetail("wsq", "Chinese", 98),
        ScoreDetail("wsq", "Math", 92),
        ScoreDetail("wsq", "English", 95),
        ScoreDetail("ade", "Chinese", 89),
        ScoreDetail("lisi", "Chinese", 63),
        ScoreDetail("ade", "Math", 98),
        ScoreDetail("zhangsan", "Math", 89),
        ScoreDetail("zhangsan", "English", 79),
        ScoreDetail("ade", "English", 89),
        ScoreDetail("lis", "English", 89),
        ScoreDetail("lis", "Chinese", 69)
    )

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("combineByKey").setMaster("local[2]")
        val sc = SparkSession.builder().config(conf).getOrCreate().sparkContext
        val scoresWithKey: Seq[(String, ScoreDetail)] = for {i <- scores} yield (i.name, i)
        val scoresWithKeyRDD: RDD[(String, ScoreDetail)] = sc.parallelize(scoresWithKey).partitionBy(new HashPartitioner(3)).cache()
        val avgSocreRDD: RDD[(String, Float)] = scoresWithKeyRDD.combineByKey(
            (x: ScoreDetail) => (x.score, 1),
            (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1),
            (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
        ).map({case(key, value) => (key, value._1 / value._2)})

        avgSocreRDD.collect().foreach(println)
    }
}

case class ScoreDetail(name: String, subject: String, score: Float)
