import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyTotalNetworkWordCount {

  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，并设置两个工作线程，批处理时间间隔 3 秒
    val sparkConf = new SparkConf().setAppName("MyTotalNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //updateStateByKey： 由于这个转换操作需要使用之前的状态信息，就需要把之前的状态信息记录下 ------> checkpoint的目录下
    ssc.checkpoint("hdfs://192.168.157.111:9000/spark/checkpoint")

    //创建一个DStream对象，并连接到netcat的服务器端
    val lines = ssc.socketTextStream("192.168.157.81", 7788, StorageLevel.MEMORY_AND_DISK_SER)

    //采集数据，并处理，分词
    val words = lines.flatMap(_.split(" "))
    //生成一个个的元组对: (word,1)
    val wordPair = words.map(word => (word,1))
    //定义一个新的函数：把当前的值跟之前的结果进行一个累加
    val addFunc = (currValues:Seq[Int],preValueState:Option[Int]) => {
      //当前当前批次的值
      val currentCount = currValues.sum

      //得到已经累加的值。如果是第一次求和，之前没有数值，从0开始计数
      val preValueCount = preValueState.getOrElse(0)

      //进行累加，然后累加后结果，是Option[Int]
      Some(currentCount + preValueCount)
    }

    //要把新的单词个数跟之前的结果进行叠加（累计）
    val totalCount = wordPair.updateStateByKey[Int](addFunc)

    //输出结果
    totalCount.print()

    //开启SparkStreaming的任务
    //启动StreamingContext，开始计算
    ssc.start()

    //等待计算结束
    ssc.awaitTermination()
  }
}






















