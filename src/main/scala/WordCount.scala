import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //从文件中创建RDD
    val rdd = sc.textFile("e:/x.txt")
    //文件中的单词用空格区分
      rdd.flatMap(_.split("\\s+"))
      .map(w => (w.toLowerCase, 1))
      .reduceByKey(_+_)
      .foreach(println)
  }
}