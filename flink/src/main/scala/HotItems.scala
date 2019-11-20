import java.net.URL
import java.sql.Timestamp
import java.{lang, util}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author: shudj
  * @time: 2019/11/19 16:36
  * @description:
  */

class UserBehavior{
    var userId: Long = _        // 用户ID
    var itemId: Long = _        // 商品ID
    var categoryId: Int = _     // 商品类目ID
    var behavior: String = _    // 用户行为，包括（'pv', 'buy', 'cart', 'fav'）
    var timestamp: Long = _     // 行为发生的时间戳，单位秒
}

object ItemViewCount {
    def of(itemId: Long, windowEnd: Long, viewCount: Long): ItemViewCount = {
        val itemViewCount = new ItemViewCount
        itemViewCount.itemId = itemId
        itemViewCount.windowEnd = windowEnd
        itemViewCount.viewCount = viewCount

        itemViewCount
    }
}

class ItemViewCount {
    var itemId: Long = _ // 商品ID

    var windowEnd: Long = _ // 窗口结束时间戳

    var viewCount: Long = _ // 商品的点击量
}


class HotItems
object HotItems {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        /**
          * ProcessingTime：事件被处理的时间。也就是由机器的系统时间来决定。
          * EventTime：事件发生的时间。一般就是数据本身携带的时间。
          */
        // 告诉 Flink 我们现在按照 EventTime 模式进行处理，Flink 默认使用 ProcessingTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 为了使打印到控制台的数据结果不乱序，配置全局的并发为1，这里改变并发对结果没有影响
        env.setParallelism(1)

        val fileUrl: URL = classOf[HotItems].getClassLoader.getResource("UserBehavior.csv")
        val path: Path = Path.fromLocalFile(new java.io.File(fileUrl.toURI))
        // 抽取UserBeHavior的TypeInformation，是一个PojoTypeInfo
        val pojoType: PojoTypeInfo[UserBehavior] = TypeExtractor.createTypeInfo(classOf[UserBehavior]).asInstanceOf[PojoTypeInfo[UserBehavior]]
        val fields: Array[String] = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")
        // 创建PojoCsvInputFormat
        val csvInput = new PojoCsvInputFormat[UserBehavior](path, pojoType, fields)

        //创建输入源
        val dataSource: DataStream[UserBehavior] = env.createInput(csvInput)
        // 抽取出时间和生成watermark
        dataSource.filter(_.behavior.equals("pv"))
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
                override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000
            })
            .keyBy("itemId")
            .timeWindow(Time.minutes(60), Time.minutes(5))
            .aggregate(new AggregateFunction[UserBehavior, Long, Long] {
                override def createAccumulator(): Long = 0L

                override def add(in: UserBehavior, acc: Long): Long = acc +1

                override def getResult(acc: Long): Long = acc

                override def merge(acc: Long, acc1: Long): Long = acc + acc1
            }, new WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
                /**
                  *
                  * @param key      // 窗口的主键，即itemId
                  * @param window   // 窗口
                  * @param input    // 聚合函数的结果，即count值
                  * @param out      // 输出类型为ItemViewCount
                  */
                override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
                    val itemId = key.asInstanceOf[Tuple1[Long]].f0
                    val count = input.iterator.next()
                    out.collect(ItemViewCount.of(itemId, window.getEnd, count))
                }
            }).keyBy("windowEnd")
            // 求某个窗口前N个的热门点击商品， key为窗口时间戳，输出TopN的结果字符串
            .process(new KeyedProcessFunction[Tuple, ItemViewCount, String] {

            val topSize = 3
            // 用于存储商品的状态，待收齐同一个窗口的数据后出发topN
            var itemState: ListState[ItemViewCount] = _

            override def open(parameters: Configuration): Unit = {
                super.open(parameters)
                val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
                itemState = getRuntimeContext.getListState(itemStateDesc)
            }
            override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
                // 每条数据都保存到状态中
                itemState.add(value)
                // 注册windowEnd+1的EventTime timer,当触发时，说明收起来属于windowEnd窗口的所有商品数据
                ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
            }

            override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
                // 获取收到的所有商品点击量
                val allItems = new util.ArrayList[ItemViewCount]()
                import collection.JavaConversions._
                val itemViewCounts: lang.Iterable[ItemViewCount] = itemState.get()
                for (item: ItemViewCount <- itemViewCounts) {
                    allItems.add(item)
                }
                // 提前清除状态中的数据，释放空间
                itemState.clear()
                // 按照点击量从大到小排序
                allItems.sort(new java.util.Comparator[ItemViewCount] {
                    override def compare(a: ItemViewCount, b: ItemViewCount): Int = (b.viewCount - a.viewCount).toInt
                })

                // 将排名信息格式化成String，便于打印
                val sb = new StringBuilder
                sb.append("==========================================\n")
                sb.append(s"时间：${new Timestamp(timestamp - 1)}\n")
                for (i <- 0 until allItems.size() if i < topSize) {
                    val currentItem = allItems.get(i)
                    sb.append(s"No${i}：商品ID = ${currentItem.itemId}，浏览量 = ${currentItem.viewCount}\n")
                }
                sb.append("==========================================\n")
                // 控制输出频率，模拟实时滚动
                Thread.sleep(2000)
                out.collect(String.valueOf(sb))
            }
        }).print()

        env.execute("Hot Item Job")
    }
    
}
