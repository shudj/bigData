package origin.file

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * @author: shudj
  * @time: 2019/11/21 10:47
  * @description:  创建相关外部数据源
  */
object OriginOuter {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 通过Kafka创建DataStream
        val kafkaInput = env.addSource(new FlinkKafkaConsumer010[String]("topic", new SimpleStringSchema(), new Properties()))

        // 通过文件创建DataStream
        val fileInput = env.readTextFile("filePath")

        // 通过fromElement创建DataStream
        val elementsInput = env.fromElements("100,1010", "120,700").map(x => {
            val y = x.split(",")
            //
            Stock(y(0).trim.toInt, y(1).trim.toInt)
        })
    }
}


case class Stock(price: Int, volume: Int) {
    override def toString: String = s"${price}_${volume}"
}

class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
    // 初始化累加器
    override def createAccumulator(): (Long, Long) = (0L, 0L)
    // 累加器的第一个参数为元素累加和，累加器的第二参数为元素数量
    override def add(in: (String, Long), acc: (Long, Long)): (Long, Long) = (acc._1 + in._2, acc._2 + 1L)
    // 输出平均值
    override def getResult(acc: (Long, Long)): Double = acc._1 / acc._2

    override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = (acc._1 + acc1._1, acc._2 + acc._2)
}
