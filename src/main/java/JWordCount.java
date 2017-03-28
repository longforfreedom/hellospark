import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by migle on 2017/3/27.
 */
public class JWordCount {
    public static void main(String[] args) {
        SparkConf  conf = new SparkConf();
        conf.setAppName("JWordCount");
        conf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("e:/x.txt");

        rdd.flatMap(line-> Arrays.asList(line.split("\\s+")))
                .mapToPair(w -> new Tuple2<String,Integer>(w.toLowerCase(),1))
                .reduceByKey((a,b)->a+b)
                .foreach(w-> System.out.println(w));
    }
}
