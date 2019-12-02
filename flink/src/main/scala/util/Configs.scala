package util

import java.io.FileInputStream
import java.util.Properties

/**
  * @author: shudj
  * @time: 2019/12/2 15:37
  * @description:
  */
object Configs {

    private val prop = {
        val prop = new Properties()
        val in = new FileInputStream(this.getClass.getClassLoader.getResource("config.properties").getPath)
        prop.load(in)

        in.close()

        prop
    }

    def getValue(key: String): String = prop.getProperty(key)
}
