package user.source

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * @author: shudj
  * @time: 2019/12/3 15:27
  * @description:
  */
class SourceFromMySQL extends RichSourceFunction[Role] {

    var ps: PreparedStatement = null
    private var conn: Connection = _

    /**
      * open() 方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = getConnection
        val sql: String = "select * from role"
        ps = conn.prepareStatement(sql)
    }

    /**
      * 程序执行完毕就可以进行，关闭连接和释放资源动作了
      */
    override def close(): Unit = {
        super.close()
        if (null != conn) {
            conn.close()
        }
        if (null != ps) {
            ps.close()
        }
    }

    /**
      * DataStream 调用一次run()方法获取数据
      * @param ctx
      */
    override def run(ctx: SourceFunction.SourceContext[Role]): Unit = {
        val rs = ps.executeQuery()
        while (rs.next()) {
            val role = new Role(rs.getInt("role_id"), rs.getString("role_name"))

            ctx.collect(role)
        }
    }

    override def cancel(): Unit = ???

    private def getConnection: Connection = {
        classOf[com.mysql.jdbc.Driver]

        DriverManager.getConnection("jdbc:mysql://localhost:3306/sku?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=GMT%2B8",
            "root",
            "julong")
    }
}
