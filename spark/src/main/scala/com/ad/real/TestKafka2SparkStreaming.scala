package com.ad.real

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
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
        val value = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe(Set(topics), null))

        value.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                rdd.foreach(line => {
                    println(line.value())
                })
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}

