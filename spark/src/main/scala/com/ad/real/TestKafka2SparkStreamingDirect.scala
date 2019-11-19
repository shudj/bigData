
package com.ad.real

import kafka.message.MessageAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: shudj
  * @time: 2019/10/11 15:27
  * @description:
  */
object TestKafka2SparkStreamingDirect {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("kafka2spark").setMaster("local[2]")
        // 10秒钟拉取一次数据
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        // 配置kafka
        val kafkaParams = Map(
            "bootstrap.servers" -> "analyze03:6667",
            "group.id" -> "kafka2Hive",
            "enable.auto.commit" -> "false"
        )
        // 设置读取的首次位置，offset
        //val offset: Map[TopicAndPartition, Long] = Map(new TopicAndPartition("employee", 0) -> 0, new TopicAndPartition("employee", 1) -> 0, new TopicAndPartition("employee", 2) -> 0)
        val offset: Map[TopicPartition, Long] = Map(new TopicPartition("employee", 0) -> 0)
        val topics: Set[String] = Set("employee")
        val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
        //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        val stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(topics, kafkaParams, offset))

        stream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd.foreachPartition(records=>{
                    while (records.hasNext){
                        val message = records.next()
                        println("message:" + message)
                    }
                })

                offsetRanges.foreach(off => {
                    println(off.topic + "\t" + off.partition + "\t" + off.fromOffset + "\t" + off.untilOffset)
                })
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }

}
