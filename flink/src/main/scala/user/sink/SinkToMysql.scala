package user.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import user.bean.Role

/**
  * @author: shudj
  * @time: 2019/12/3 16:18
  * @description:
  */
class SinkToMysql extends RichSinkFunction[Role]{

    var state: PreparedStatement = null
    private var conn: Connection = _

    /**
      * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = getConnection
        val sql = "insert into role(role_name) values(?)"
        state = conn.prepareStatement(sql)
    }

    /**
      * 关闭连接和释放资源
      */
    override def close(): Unit = {
        super.close()
        if (null != conn) {
            conn.close()
        }

        if (null != state) {
            state.close()
        }
    }

    /**
      * 每条数据的插入都要调用一次 invoke() 方法
      * @param value
      * @param context
      */
    override def invoke(value: Role, context: SinkFunction.Context[_]): Unit = {
        try {
            // 组装数据，执行插入操作
            state.setString(1, value.roleName)

            state.executeUpdate()
        } catch {
            case e: Exception => e.printStackTrace()
        }
    }

    private def getConnection: Connection = {
        classOf[com.mysql.cj.jdbc.Driver]
        DriverManager.getConnection("jdbc:mysql://localhost:3306/sku?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=GMT%2B8",
            "root",
            "julong")
    }
}
