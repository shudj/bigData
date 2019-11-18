import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @author: shudj
  * @time: 2019/9/18 14:55
  * @description:
  */
object WordCount_offline {

    def main(args: Array[String]): Unit = {
        // 初始化环境
        val env = ExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        val text = env.fromElements("Who`s there?", "I think I hear them.999 stand, ho! Who` there?")
        // 分割字符串、汇总tuple、按照key进行分组，统计分组后word个数
        val counts = text.flatMap(_.toLowerCase.split("\\W+") filter(_.nonEmpty))
            .map((_, 1))
            .groupBy(0)
            .sum(1)

        // 打印
        counts.print()

        "ni hao a".split("\\W+").foreach(println)
    }
}
