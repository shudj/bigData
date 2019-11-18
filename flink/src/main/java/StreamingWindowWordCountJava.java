import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.omg.CORBA.TIMEOUT;

/**
 * @author: shudj
 * @time: 2019/9/18 14:31
 * @description:
 */
public class StreamingWindowWordCountJava {
    public static void main(String[] args) throws Exception {

        // 定义端口
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("没有指定port参数，使用默认值9000");
            port = 9000;
        }

        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("node21", port, "\n");
        // 计算数据，打平操作，把每行的单词转为<word, count>类型的数据
        DataStream<WordWithCount> withCountDataStream = text.flatMap((FlatMapFunction<String, WordWithCount>) (s, collector) -> {
            String[] splits = s.split("\\s");
            for (String word: splits) {
                collector.collect(new WordWithCount(word, 1L));
            }
        })
                // 针对相同的word数据进行分组
                .keyBy("word")
                // 指定计算数据的窗口大小和滑动窗口大小
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .sum("count");

        // 把数据打印到控制台，使用一个并行度
        withCountDataStream.print().setParallelism(1);
        // 注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");


    }

    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount {" + "word ='" + this.word + "', count = '" + this.count + "'}";
        }
    }
}

