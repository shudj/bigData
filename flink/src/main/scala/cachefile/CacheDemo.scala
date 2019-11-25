package cachefile

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.io.Source

/**
  * @author: shudj
  * @time: 2019/11/25 16:12
  * @description:
  * Flink提供了一个分布式缓存，类似于Apache Hadoop，可以在本地访问用户函数的并行实例。此函数可用于共享包含静态外部数据的文件，如字典或机器学习的回归模型。
  * 缓存的工作原理如下。程序在其作为缓存文件的特定名称下注册本地或远程文件系统（如HDFS或S3）的文件或目录ExecutionEnvironment。执行程序时，Flink会自动将文件或目录复制到所有工作程序的本地文件系统。用户函数可以查找指定名称下的文件或目录，并从worker的本地文件系统访问它。
  * 其实分布式缓存就相当于spark的广播,把一个变量广播到所有的executor上,也可以看做是Flink的广播流,只不过这里广播的是一个文件.
  *
  * 作者：JasonLee_
  * 链接：https://www.jianshu.com/p/5753b5f0bd76
  * 来源：简书
  * 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
  */
object CacheDemo {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(3)
        // 注册缓存的文件，文件理由数据
        env.registerCachedFile("C:/Users/admin/Desktop/wsq.txt", "name")
        env.fromElements("hello", "jason", "hello", "wsq").map(new RichMapFunction[String, String] {
            var list: List[(String)] = _


            override def open(parameters: Configuration): Unit = {
                super.open(parameters)
                val file = this.getRuntimeContext.getDistributedCache.getFile("name")
                val lines = Source.fromFile(file.getAbsoluteFile).getLines()
                list = lines.toList
            }

            override def map(in: String): String = {
                var middle: String = ""
                if (list(0).contains(in)) {
                    middle = in
                }

                middle
            }
        }).map((_, 1L))
            .filter(_._1.nonEmpty)
            .groupBy(0)
            .sum(1)
            .print()
    }
}
