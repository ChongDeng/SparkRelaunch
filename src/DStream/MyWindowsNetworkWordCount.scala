package DStream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyWindowsNetworkWordCount {
  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，并设置两个工作线程，批处理时间间隔 1 秒
    val sparkConf = new SparkConf().setAppName("MyWindowsNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //创建一个DStream对象，并连接到netcat的服务器端
    val lines = ssc.socketTextStream("192.168.157.81", 7788, StorageLevel.MEMORY_AND_DISK_SER)

    //采集数据，并处理
    val words = lines.flatMap(_.split(" "))

    //val wordPair = words.map(x => (x, 1))
    //使用transform转换算子来生成一个元组对
    val wordPair = words.transform(x=>x.map(x=>(x,1)))

    //val wordCounts = wordPair.reduceByKey(_ + _)
    //使用窗口操作：每10秒对过去30秒内的数据进行单词计算
    val wordCounts = wordPair.reduceByKeyAndWindow((a:Int,b:Int) => (a+b),Seconds(30),Seconds(10))

    //打印结果
    wordCounts.print()

    //启动StreamingContext，开始计算
    ssc.start()
    //等待计算结束
    ssc.awaitTermination()
  }
}
