package com.ad.study

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: shudj
  * @time: 2019/11/6 16:04
  * @description:
  */
object Spark_demo01 {
    def main(args: Array[String]): Unit = {
        // 初始化Spark
        val conf = new SparkConf().setAppName("demo01").setMaster("local[2]")
        val sc = new SparkContext(conf)
        /**
         *  弹性分布式数据集
         */
        /**
          * 并行集合
          */
        val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)
        distData.reduce((a, b) => a + b)

        /**
          * 外部数据集
          *     Spark可以从Hadoop所支持的任何存储源中创建distributed dataset(分布式数据集)，包括本地文件系统，HDFS(hdfs://)、Cassandra、HBase、
          *     Amazon S3(s3n://) 等，Spark支持文本文件，SequenceFiles，以及任何其他的Hadoop InputFormat
          *
          *  注意事项：
          *     ·如果使用本地文件系统的路径，所有工作节点的相同访问路径下改文件必须可以访问，即复制改文件到所有的工作节点上，或者使用共享网络挂载文件系统
          *     ·所有Spark基于文件的input方法，包括textFile，支持在目录上运行，压缩文件，和通配符。如：textFile("/my/dir"), textFile("/my/dir\\*.txt"),
          *      textFile("/my/dir\\*.gz")
          *
          * 除了文本文件之外，Spark 的 Scala API 也支持一些其它的数据格式:
          *     ·SparkContext.wholeTextFiles 可以读取包含多个小文本文件的目录，并且将它们作为一个 (filename, content) pairs 来返回。这与 textFile 相比，
          *      它的每一个文件中的每一行将返回一个记录。分区由数据量来确定，某些情况下，可能导致分区太少。针对这些情况，wholeTextFiles 在第二个位置提供了一个可选的参数用户控制分区的最小数量.
          *     ·针对 SequenceFiles，使用 SparkContext 的 sequenceFile[K, V] 方法，其中 K 和 V 指的是文件中 key 和 values 的类型。这些应该是 Hadoop 的 Writable 接口的子类，
          *      像 IntWritable and Text。此外，Spark 可以让您为一些常见的 Writables 指定原生类型; 例如，sequenceFile[Int, String] 会自动读取 IntWritables 和 Texts.
          *     ·针对其它的 Hadoop InputFormats，您可以使用 SparkContext.hadoopRDD 方法，它接受一个任意的 JobConf 和 input format class，key class 和 value class。
          *      通过相同的方法你可以设置你的 input source（输入源）。你还可以针对 InputFormats 使用基于 “new” MapReduce API（org.apache.hadoop.mapreduce）的 SparkContext.newAPIHadoopRDD.
          *     ·RDD.saveAsObjectFile 和 SparkContext.objectFile 支持使用简单的序列化的 Java objects 来保存 RDD。虽然这不像 Avro 这种专用的格式一样高效，
          *      但其提供了一种更简单的方式来保存任何的 RDD。
          */
        val distFile = sc.textFile("data.txt")
        distFile.map(s => s.length).reduce(_+_)
    }
}
