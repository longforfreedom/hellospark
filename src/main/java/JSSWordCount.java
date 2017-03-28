import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by migle on 2017/3/28.
 */
public class JSSWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("JSSWordCount");
        conf.setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(20));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("helloss",1);
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, "vm-centos-00:2181","ss-group",topicMap);
        JavaPairDStream<String, Integer> r = messages.map(x -> x._2())
                .flatMap(line -> Arrays.asList(line.split("\\s+")))
                .mapToPair(w -> new Tuple2<String, Integer>(w.toLowerCase(), 1))
                .reduceByKey((a, b) -> a + b);
        r.print(10);
        jssc.start();
        jssc.awaitTermination();
    }
}
