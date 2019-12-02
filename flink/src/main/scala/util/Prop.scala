package util

/**
  * @author: shudj
  * @time: 2019/12/2 15:42
  * @description:
  */
object Prop {

    val BOOTSTRAP_SERVERS: String = Configs.getValue("bootstrap.servers")
    val ZOOKEEPER_CONNECT: String = Configs.getValue("zookeeper.connect")
    val GROUP_ID: String = Configs.getValue("group.id")

}
