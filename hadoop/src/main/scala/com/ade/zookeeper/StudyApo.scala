package com.ade.zookeeper

import com.ade.util.Configs
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * @author: shudj
  * @time: 2019/11/26 15:08
  * @description:
  */
object StudyApo {

    private val path = "/test_ad"
    def main(args: Array[String]): Unit = {

        getData
        setData("ade")
        getData
    }

    val zk: ZooKeeper = new ZooKeeper(Configs.ZOOKEEPER_ADDRESS, Configs.ZOOKEEPER_SESSION_TIMEOUT,
        new Watcher {
            override def process(event: WatchedEvent): Unit = {
                println(s"时间触发----：${event.getType}")
            }
        })

    def create: Unit = {
        /**
          * 第一个参数：路径
          * 第二个参数：值 bytes
          * 第三个参数：对节点的访问控制
          * 第四个参数：对节点的类型 短暂 永久 序号
          */
        val str = zk.create(path + "/ade1", "ade".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

        println(str)
    }

    def exist: Unit = {
        // 设置为true会调用zookeeper的监听器
        val stat = zk.exists(path, true)
        if (null != stat) {
            println(s"存在 --> ${stat.getDataLength}")
        } else {
            println("不存在")
        }
    }

    def getChildren: Unit = {
        import scala.collection.JavaConverters._
        val list = zk.getChildren(path, true).asScala
        if (null != list && !list.isEmpty) {
            list.foreach(println)
        }
    }

    def getData: Unit = {
        val bytes = zk.getData(path, new Watcher {
            override def process(event: WatchedEvent): Unit = {
                println(s"数据变了 --> ${event.getType}")
            }
        }, null)
        println(new String(bytes))
        Thread.sleep(Long.MaxValue)
    }

    def setData(str: String): Unit = {
        // version -1 自动维护
        val stat = zk.setData(path, str.getBytes(), -1)
        println(stat.getCversion)
    }

    def deleteNode: Unit = {
        zk.delete(path, -1)
    }
}
