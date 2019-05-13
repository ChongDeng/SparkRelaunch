package DStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MySocketDStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDQueueInputStream").setMaster("local[2]")
    //每1秒对数据进行处理，监听Socket端口
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //创建一个DStream，监听Socket地址：localhost:8899
    val lines = ssc.socketTextStream("localhost",8899)
    //打印
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
