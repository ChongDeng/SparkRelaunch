import org.apache.spark.{SparkConf, SparkContext}

//使用Scala语言开发自己的WordCOunt程序
object WordCountScala {
  def main(args: Array[String]): Unit = {
    //主方法

    //创建一个SparkConf对象
    val conf = new SparkConf().setAppName("Scut Scala WordCount")

    //创建SparkContext
    val sc = new SparkContext(conf)

    //使用sc对象创建RDD: transformation  action
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile(args(1))

    //结束任务
    sc.stop()
  }
}
