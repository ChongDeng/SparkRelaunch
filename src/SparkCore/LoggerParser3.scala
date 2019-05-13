package SparkCore

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object LoggerParser3 {
  //统计一天中每个小时间的数据流量
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")

    //创建一个SparkConf对象
    val conf = new SparkConf().setAppName("Scut Scala WordCount").setMaster("local")
    //创建SparkContext
    val sc = new SparkContext(conf)

    //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
    val  timePattern=".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r
    //匹配 http 响应码和请求数据大小
    val httpSizePattern=".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

    def  isMatch(pattern:Regex,str:String)={
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }

    sc.textFile("C:\\D_study\\hadoop&spark\\spark_qiniu_log.txt")
      .filter(x=>isMatch(httpSizePattern,x))
      .filter(x=>isMatch(timePattern,x))
      .map(line =>{
          var time =  "(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}".r.findFirstIn(line).toString()
          var hour_time = time.split(":")(2)

          var data_info = "(200|206|304)\\s([0-9]+)".r.findFirstIn(line).toString().trim()
          var pos = data_info.indexOf(" ")
          var data_len = data_info.substring(pos + 1, data_info.size - 1).toDouble
          (hour_time, data_len)
      })
      .groupByKey()
      .map(x=>(x._1,x._2.sum))
      .sortByKey().
      foreach(x=>println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"G"))

    //结束任务
    sc.stop()
  }
}
