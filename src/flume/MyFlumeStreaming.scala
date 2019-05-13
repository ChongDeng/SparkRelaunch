package flume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyFlumeStreaming {
  def main(args: Array[String]): Unit = {
    //创建一个SparkStreaming Context对象
    val sparkConf = new SparkConf().setAppName("MyFlumeStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建FlumeEvent的DStream，采用push方式读取flume推送过来的数据
    //val flumeEvent = FlumeUtils.createStream(ssc,"192.168.1.88",1234)

    //创建FlumeEvent的DStream，采用pull方式读取flume
    val flumeEvent = FlumeUtils.createPollingStream(ssc,"192.168.1.88",1234,
      StorageLevel.MEMORY_AND_DISK_SER_2)

    //接收数据 把Flume Event中的事件转换成是一个字符串
    val lineDStream = flumeEvent.map(e=>{
      new String(e.event.getBody.array())
    })

    //输出结果
    lineDStream.print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
