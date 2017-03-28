import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by migle on 2017/3/28.
  */
  object SSWordCount {
    def main(args: Array[String]): Unit = {
      //方便起间，程序中写死以local方式运行
      val sparkConf = new SparkConf().setAppName("SSWordCount").setMaster("local[2]")
      //每10秒钟统计一次接收到单词数
      val ssc = new StreamingContext(sparkConf, Seconds(10))
      val topicMap = Map("helloss"-> 1)
      val messages = KafkaUtils.createStream(ssc,"vm-centos-00:2181","ss-group",topicMap)
      val r = messages.map(_._2).flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
      //只打印10记录，实际中一般会保存到HDFS，Redis,Kafka中
      //spark streaming需要一个Output Operations来触发执行，否则再多的Transformations也不会执行
      r.print(10)
      //启动Streaming程序
      ssc.start()
      ssc.awaitTermination()
    }
  }

