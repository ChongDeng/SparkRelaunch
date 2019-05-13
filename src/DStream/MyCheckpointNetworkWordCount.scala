package DStream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyCheckpointNetworkWordCount {
  def main(args: Array[String]): Unit = {
    //在主程序中，创建一个Streaming Context对象
    //1、读取一个检查点的目录
    //2、如果该目录下已经存有之前的检查点信息，从已有的信息上创建这个Streaming Context对象
    //3、如果该目录下没有信息，创建一个新的Streaming Context
    val context = StreamingContext.getOrCreate("hdfs://192.168.157.111:9000/spark_checkpoint",
      createStreamingContext)

    //启动任务
    context.start()
    context.awaitTermination()
  }

  //创建一个StreamingContext对象，并且设置检查点目录，执行WordCount程序（记录之前的状态信息）
  def createStreamingContext():StreamingContext = {
    val conf = new SparkConf().setAppName("MyCheckpointNetworkWordCount").setMaster("local[2]")
    //创建这个StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //设置检查点目录
    ssc.checkpoint("hdfs://192.168.157.111:9000/spark_checkpoint")

    //创建一个DStream，执行WordCount
    val lines = ssc.socketTextStream("192.168.157.81",7788,StorageLevel.MEMORY_AND_DISK_SER)

    //分词操作
    val words = lines.flatMap(_.split(" "))
    //每个单词记一次数
    val wordPair = words.map(x=> (x,1))

    //执行单词计数
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

    //返回这个对象
    ssc
  }
}














