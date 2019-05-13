package kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")

    //创建一个Spark StreamingContext对象
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //创建Topic的名字: "scut_demo1"，每次从这个topic中接收的数据条数
    val topics = Map("scut_demo1" -> 1)

    //创建一个Kafka的输入流DStream ,指定ZK地址
    val kafkaStream = KafkaUtils.createStream(ssc,"192.168.1.65:2181","scut_group",topics)

    //读取数据
    val lineStream = kafkaStream.map(e => {
      new String(e.toString())
    })

    lineStream.print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}


















