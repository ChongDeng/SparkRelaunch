package SparkSQL

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object MyCSVParser2 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")

    //创建sparkSession
    val sparkSession = SparkSession.builder.config(sparkConf).master("local")
                                   .appName("spark session example").getOrCreate()

    //加载结构化数据
    val path = "C:\\D_study\\hadoop&spark\\cdn.csv"
    //读取文件
    //val df = sparkSession.read.option("header", "true").csv(path)
    val df = sparkSession.read.csv(path)
    //将加载的数据临时命名为log
    df.createOrReplaceTempView("log")

    def getHour(time:String)={
      val date=new Date(Integer.valueOf(time)*1000);
      val sf=new SimpleDateFormat("HH");
      sf.format(date)
    }

    //查询每个小时视频流量
    val hourCdnSQL="select _c4,_c8 from log "
    //取出时间和大小将格式化时间，RDD中格式为 (小时,大小)
    val dataRdd= sparkSession.sql(hourCdnSQL)
                             .rdd.map(row=>Row(getHour(row.getString(0)),
                                           java.lang.Long.parseLong(row.get(1).toString)))

    val schema=StructType(
      Seq(
          StructField("hour",StringType,true),
          StructField("size",LongType,true)
      )
    )

    //将dataRdd转成DataFrame
    val peopleDataFrame = sparkSession.createDataFrame(dataRdd,schema)
    peopleDataFrame.createOrReplaceTempView("cdn")
    //按小时分组统计
    val results = sparkSession.sql("SELECT hour, sum(size) as size FROM cdn group by hour order by hour ")
    results.foreach(row=>println(row.get(0)+"时 流量:"+row.getLong(1)/(1024*1024*1024)+"G"))
  }
}
