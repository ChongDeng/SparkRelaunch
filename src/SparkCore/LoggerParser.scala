package SparkCore

import org.apache.spark.{SparkConf, SparkContext}

object LoggerParser {
  //计算独立IP数； IP访问量top 10
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")

    //创建一个SparkConf对象
    val conf = new SparkConf().setAppName("Scut Scala WordCount").setMaster("local")

    //创建SparkContext
    val sc = new SparkContext(conf)

    val  IPPattern="((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

    val ipNums= sc.textFile("C:\\D_study\\hadoop&spark\\spark_qiniu_log.txt")
                .flatMap(x=>IPPattern findFirstIn(x))
                .map((_,1)).reduceByKey(_+_).sortBy(_._2,false)
    println("独立IP数："+ipNums.count())
    println("top 10 ip: ===========")
    ipNums.take(10).foreach(println)

    //结束任务
    sc.stop()
  }
}
