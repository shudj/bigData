import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
  * @author: shudj
  * @time: 2019/11/25 10:50
  * @description:
  */
object StudyDataSet {

    private val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tArr: Array[(Int, String, Int)] = Array(
            (1, "ade", 3), (2, "ade", 5),
            (3, "wsq", 10), (2, "wsq", 3),
            (3, "wsq", 1)
    )
    val pArr = Array(
            (BookPojo("ade", "第一天", 1000L), 2D),
            (BookPojo("ade", "第二天", 500L), 4D),
            (BookPojo("wsq", "二八芳华", 1200L), 5D),
            (BookPojo("wsq", "青春", 1200L), 8D)
    )

    val sArr = Array("ade","admin", "wsq", "wade")

    def main(args: Array[String]): Unit = {
        //studySortPartition
        studyAggregate
    }

    def studySortPartition: Unit = {
        val tData: DataSet[(Int, String, Int)] = env.fromCollection(tArr)
        // 根据String属性进行排列
        println("根据String属性进行排列")
        tData.sortPartition(1, Order.DESCENDING).print()
        println("先根据第三个Int升序，第一个int降序")
        tData.sortPartition(2, Order.ASCENDING).sortPartition(0, Order.DESCENDING).print()

        val pData: DataSet[(BookPojo, Double)] = env.fromCollection(pArr)
        println("根据BookPojo的author属性降序排列")
        pData.sortPartition("_1.author", Order.DESCENDING).writeAsText("C:/Users/admin/Desktop/wsq1.txt")

        val sData = env.fromCollection(sArr)
        println("根据所有属性升序排列")
        tData.sortPartition("_", Order.ASCENDING).writeAsText("C:/Users/admin/Desktop/wsq2.txt")

        env.execute()
    }

    def studyAggregate: Unit = {
        val tData = env.fromCollection(tArr)
        tData.groupBy(1).aggregate(Aggregations.SUM, 0).and(Aggregations.MAX, 2).print()
    }
}


case class BookPojo(var author: String, var name: String, var page: Long)