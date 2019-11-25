package broadcast

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.collection.mutable._

/**
  * @author: shudj
  * @time: 2019/11/25 14:51
  * @description:
  */
object BroadCastDemo {

    val broadData = Array(("zs", 18), ("sq", 17), ("ade", 19))

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        // 处理需要广播的数据，把数据转换成map类型
        val toBroadCast = env.fromCollection(broadData).map(value => Map(value._1 -> value._2))

        val data = env.fromElements("zs", "sq", "ade", "admin")
        // 在这里需要使用到RichMapFunction获取广播变量
        data.map(new RichMapFunction[String, String] {

            var allMap: mutable.Map[String, Int] = Map[String, Int]()
            private var broadCastMap: mutable.Buffer[Map[String, Int]] = Buffer[Map[String, Int]]()

            /**
              * 这个函数只会执行一次
              * 可以在这里实现一些初始化的功能
              * 所以，就可以在open方法中获取广播变量数据
              * @param parameters
              */
            override def open(parameters: Configuration): Unit = {
                super.open(parameters)
                import scala.collection.JavaConverters._
                // 获取数据
                broadCastMap = this.getRuntimeContext.getBroadcastVariable[Map[String, Int]]("name").asScala
                broadCastMap.foreach(map => {
                    allMap = allMap ++ map
                })
            }

            override def map(in: String): String = {
                val value = allMap.get(in)

                s"${in}, ${value.getOrElse(0)}"
            }
            // 执行广播数据的额操作
        }).withBroadcastSet(toBroadCast, "name").print()
    }
}
