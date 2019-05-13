import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileInputStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //从本地目录中读取数据，如果有新的文件产生，就会读取进来
    val lines = ssc.textFileStream("D:\\vidoes\\input")

    //打印读入的数据
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
