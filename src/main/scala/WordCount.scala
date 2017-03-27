import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("")
  }
}