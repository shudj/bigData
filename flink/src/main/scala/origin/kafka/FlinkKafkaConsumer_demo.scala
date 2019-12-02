package origin.kafka

import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import util.Prop

/**
  * @author: shudj
  * @time: 2019/12/2 15:33
  * @description:
  */
object FlinkKafkaConsumer_demo {

    val prop = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", Prop.BOOTSTRAP_SERVERS)
        properties.setProperty("zookeeper.connect", Prop.ZOOKEEPER_CONNECT)
        properties.setProperty("group.id", Prop.GROUP_ID)
        properties.setProperty("auto.commit.enable", "true")
        properties.setProperty("auto.commit.interval.ms", "1000")
        properties.setProperty("session.timeout.ms", "30000")
        // 动态感知kafka主题分区的增加
        properties.setProperty("flink.partition.discovery.interval.millis", "5000")
        /**
          * 用来限制每次consumer fetch数据的大小限制，只是限制partition的，无法限制到一次拉取的总量。
          */
        properties.put("max.partition.fetch.bytes", "10485760");
        properties.put("fetch.message.max.bytes", "3072000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

        properties
    }

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.enableCheckpointing(1000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("employee", new SimpleStringSchema(), prop))
        kafkaStream.map(str => {
            print(new Date())
            println(str)
        })

        env.execute()
    }

}
