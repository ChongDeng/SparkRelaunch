import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkWordCount {
  def main(args: Array[String]): Unit = {
    //创建一个Context对象: StreamingContext  (SparkContext, SQLContext)
    //指定批处理的时间间隔
    val conf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //创建一个DStream，处理数据
    val lines  = ssc.socketTextStream("192.168.88.81",7788,StorageLevel.MEMORY_AND_DISK_SER)

    //执行wordcount
    val words = lines.flatMap(_.split(" "))
    val wordCountResult = words.map(x => (x,1)).reduceByKey(_ + _)

    //输出结果
    wordCountResult.print()

    //启动StreamingContext
    ssc.start()

    //等待计算完成
    ssc.awaitTermination()
  }
}
