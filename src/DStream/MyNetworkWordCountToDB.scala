package DStream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkWordCountToDB {
  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，并设置两个工作线程，批处理时间间隔 3 秒
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCountToDB").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建一个DStream对象，并连接到netcat的服务器端
    val lines = ssc.socketTextStream("192.168.157.81", 7788, StorageLevel.MEMORY_AND_DISK_SER)

    //采集数据，并处理
    val words = lines.flatMap(_.split(" "))

    //val wordPair = words.map(x => (x, 1))
    //使用transform转换算子来生成一个元组对
    val wordPair = words.transform(x=>x.map(x=>(x,1)))

    val wordCounts = wordPair.reduceByKey(_ + _)

    wordCounts.foreachRDD(rdd =>{
      //取出RDD的每个分区，并在每个分区上创建数据库的链接
      rdd.foreachPartition(partionRecords =>{
        var conn:Connection = null
        var pst:PreparedStatement = null

        try{
          //在该分区上得到一个connection
          conn = DriverManager.getConnection(
            "jdbc:oracle:thin:@192.168.157.101:1521/orcl.example.com","SCOTT","tiger")
          //取出该分区上的每条记录
          partionRecords.foreach(record =>{
            //创建SQL的运行环境
            pst = conn.prepareStatement("insert into myresult values(?,?)")
            pst.setString(1,record._1)
            pst.setInt(2,record._2)
            //执行
            pst.executeUpdate()
          })
        }catch{
          case e1:Exception=> e1.printStackTrace()
        }finally {
          if(pst != null) pst.close()
          if(conn != null) conn.close()
        }
      })
    })
    //启动StreamingContext，开始计算
    ssc.start()
    //等待计算结束
    ssc.awaitTermination()
  }
}


















