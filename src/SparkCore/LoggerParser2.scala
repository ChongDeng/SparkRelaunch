package SparkCore

import org.apache.spark.{SparkConf, SparkContext}

object LoggerParser2 {
  //统计每个视频访问的独立IP数
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")

    //创建一个SparkConf对象
    val conf = new SparkConf().setAppName("Scut Scala WordCount").setMaster("local")
    //创建SparkContext
    val sc = new SparkContext(conf)

    //匹配ip地址
    val IPPattern="((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r
    //匹配文件名
    val  fileNamePattern="([0-9]+).mp4".r
    //返回(文件名，IP)这种格式
    def getFileNameAndIp(line:String)={
      (fileNamePattern.findFirstIn(line),IPPattern.findFirstIn(line))
    }

    //2.统计每个视频独立IP数
    sc.textFile("C:\\D_study\\hadoop&spark\\spark_qiniu_log.txt")
      .filter(_.matches(".*([0-9]+)\\.mp4.*"))
      .map(x=>getFileNameAndIp(x))
      .groupByKey()
      .map(x=>(x._1, x._2.toList.distinct))
      .sortBy(_._2.size,false)
      .take(10).foreach(x=>println("视频："+x._1+" 独立IP数:"+x._2.size))

    //结束任务
    sc.stop()
  }
}
