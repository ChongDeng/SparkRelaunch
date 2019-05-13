package DStream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueueInputStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDQueueInputStream").setMaster("local[2]")
    //每1秒对数据进行处理，从RDD队列中采集数据
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //创建一个RDD队列，把该队列中的RDD push到QueueInputDStream
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //基于该RDD的队列创建一个输入流
    val inputStream = ssc.queueStream(rddQueue)

    //对接受到的RDD数据进行处理：乘以10
    val mappedStream = inputStream.map(x=> (x,x*10))
    mappedStream.print()

    ssc.start()

    //对RDD的队列进行初始化
    for(i<- 1 to 3){
      rddQueue += ssc.sparkContext.makeRDD(1 to 10)
      Thread.sleep(1000)
    }

    ssc.awaitTermination()
  }
}
