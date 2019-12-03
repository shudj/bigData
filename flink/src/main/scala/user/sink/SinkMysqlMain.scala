package user.sink

import org.apache.flink.streaming.api.scala._
import user.bean.Role
import user.source.SourceFromMySQL

/**
  * @author: shudj
  * @time: 2019/12/3 16:32
  * @description:
  */
object SinkMysqlMain {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[Role] = env.addSource(new SourceFromMySQL)
            .setParallelism(1)
            .filter(_.roleId == 18)
            .map(role => new Role(role.roleId, role.roleName))

        //val dataStream = env.fromElements(new Role(19, "456")).setParallelism(1)
        dataStream.addSink(new SinkToMysql)

        env.execute("mysql sink flink")
    }
}
