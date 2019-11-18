/*
package com.ad.real

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: shudj
  * @time: 2019/9/26 16:45
  * @description:
  */
object TestKafka2SparkStreaming {

    def main(args: Array[String]): Unit = {
        val zkQuorum = "dpnode03:2181,dpnode04:2181,dpnode05:2181"
        val group = "kafka2stream"
        val numThreads = "3"
        val topics = "customerViewDetail"
        val conf = new SparkConf().setAppName("kafka_stream").setMaster("local[2]")
        conf.set("spark.eventLog.enabled", "false")
        val ssc = new StreamingContext(conf, Seconds(2))
        //ssc.checkpoint("WALCheckpoint")
        ssc.sparkContext.setLogLevel("WARN")
        val topicMap = topics.split(",").map(topic => (topic, numThreads.toInt)).toMap
        val value = KafkaUtils.createDirectStream(ssc, zkQuorum, group, topicMap)

        value.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                rdd.foreach(line => {
                    println(line._2)
                })
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
*/
