package user.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author: shudj
  * @time: 2019/12/3 15:40
  * @description:
  */
object MysqlSource {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.addSource(new SourceFromMySQL).print()

        env.execute("mysql flink source")
    }
}
