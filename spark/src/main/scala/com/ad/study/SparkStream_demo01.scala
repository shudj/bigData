package com.ad.study

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: shudj
  * @time: 2019/11/6 17:50
  * @description:
  */
object SparkStream_demo01 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SparkStreaming_demo01").setMaster("local[2]")
        // 构建一个一秒钟的SparkStreaming
        val ssc = new StreamingContext(conf, Seconds(1))
        // 创建一个将要连接到 hostname:port 的 DStream，如 localhost:9999
        val lines = ssc.socketTextStream("localhost", 9999)
        val wordCount = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
        wordCount.print()

        // 开始计算
        ssc.start()
        // 等待计算被中断
        ssc.awaitTermination()
    }
}
