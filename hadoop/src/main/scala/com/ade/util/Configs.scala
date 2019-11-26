package com.ade.util

/**
  * @author: shudj
  * @time: 2019/11/26 15:20
  * @description:
  */
object Configs {

    val ZOOKEEPER_ADDRESS = ConfigProperties.getValue("zookeeper.host")
    val ZOOKEEPER_SESSION_TIMEOUT = ConfigProperties.getValue("zookeeper.session.timeout").toInt
}
