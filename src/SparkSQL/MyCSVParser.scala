package SparkSQL

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object MyCSVParser {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")

    //创建sparkSession
    val sparkSession = SparkSession.builder
      .config(sparkConf)
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    //加载结构化数据
    val path = "C:\\D_study\\hadoop&spark\\cdn.csv"
    //读取文件
    //val df = sparkSession.read.option("header", "true").csv(path)
    val df = sparkSession.read.csv(path)
    //将加载的数据临时命名为log
    df.createOrReplaceTempView("log")
    df.show(5)

    val allIpCountSQL= "select count(DISTINCT _c1)   from log "
    val ipCountSQL= "select _c1 as IP,count(_c1) as ipCount    " +
                    "from log group by _c1 order by ipCOunt desc   limit 10"
    //查询独立IP总数
    sparkSession.sql(allIpCountSQL).foreach(row=>println("独立IP总数:"+row.get(0)))
    //查看IP数前10
    sparkSession.sql(ipCountSQL).foreach(row=>println("IP:"+row.get(0)+" 次数:"+row.get(1)))

  }
}
