package com.ade.util

import java.io.FileInputStream
import java.util.Properties

/**
  * @author: shudj
  * @time: 2019/11/26 15:00
  * @description:
  */
object ConfigProperties {

    private val prop = {
        val property = new Properties()
        val fileInputStream = new FileInputStream(this.getClass.getClassLoader.getResource("config.properties").getPath)
        property.load(fileInputStream)

        fileInputStream.close()
        property
    }

    def getValue(key: String): String = prop.getProperty(key)
}
