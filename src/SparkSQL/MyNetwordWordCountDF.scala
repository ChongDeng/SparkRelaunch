package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetwordWordCountDF {
  def main(args: Array[String]): Unit = {
    //创建一个本地的StreamingContext，并设置两个工作线程，批处理时间间隔 3 秒
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建一个DStream对象，并连接到netcat的服务器端
    val lines = ssc.socketTextStream("192.168.157.81", 7788, StorageLevel.MEMORY_AND_DISK_SER)

    //采集数据，并处理
    val words = lines.flatMap(_.split(" "))

    //使用DataFrame和SQL执行数据的处理
    //使用Spark SQL来查询Spark Streaming处理的数据
    words.foreachRDD(rdd =>{
      //使用单例模式来创建一个SparkSession对象
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()

      import spark.implicits._

      //将RDD[String]转换为DataFrame
      //word是schema结构
      val wordDataFrame = rdd.toDF("word")

      //创建临时的视图
      wordDataFrame.createOrReplaceTempView("words")

      //执行查询
      val myresult = spark.sql("select word,count(*) as total from words group by word")
      myresult.show()
    })

    //启动StreamingContext，开始计算
    ssc.start()

    //等待计算结束
    ssc.awaitTermination()
  }
}















