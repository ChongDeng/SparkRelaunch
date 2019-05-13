import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

object MyWebDemoPartitioner {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")

    //创建spacrkconf对象
    val conf = new SparkConf().setAppName("My WebCount Demo").setMaster("local")

    //创建sparkcontext
    val sc = new SparkContext(conf)

    //读入数据: 192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/head.jsp HTTP/1.1" 200 713
    val rdd1 = sc.textFile("C:\\D_study\\hadoop&spark\\localhost_access_log.2015-07-30.txt").map(
      line => {
        //解析字符串:
        val index1 = line.indexOf("\"")
        val index2 = line.lastIndexOf("\"")
        val line1 = line.substring(index1+1,index2) // 得到：GET /MyDemoWeb/head.jsp HTTP/1.1

        //得到：/MyDemoWeb/head.jsp
        val index3 = line1.indexOf(" ")
        val index4 = line1.lastIndexOf(" ")
        val line2 = line1.substring(index3+1,index4)  ///得到：MyDemoWeb/head.jsp

        //得到jsp的名字
        val line3 = line2.substring(line2.lastIndexOf("/") + 1) //得到： head.jsp

        //返回一个元组:  (head.jsp,访问的日志信息)
        (line3,line)
      }
    )

    //得到所有的key：jsp的文件名称 ----> 建立分区规则
    val rdd2 = rdd1.map(_._1).distinct().collect()

    //创建自己的分区规则
    val myPartRole = new MyPartitionerRule(rdd2)

    //进行分区
    val rdd3 = rdd1.partitionBy(myPartRole)
    //rdd3.saveAsTextFile("d://myoutput")

    println(rdd1.collect.toBuffer)
    //将rdd1的数据保存进入Oracle
    rdd1.foreachPartition(saveToOracle)

    sc.stop()
  }

  //通过RDD的分区的迭代器: 返回素有的数据
  //数据存入Oracle
  def saveToOracle(it:Iterator[(String,String)]) ={
    var conn:Connection = null
    var ps:PreparedStatement = null

    try{
        //得到数据库的链接
      conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.88.101:1521/orcl.example.com","scott","tiger")

      //得到SQL的运行环境
      ps = conn.prepareStatement("insert into mylogdata values(?,?)")

      //将一个分区中的所有元素插入Oracle
      it.foreach(
          //(head.jsp,192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/head.jsp HTTP/1.1" 200 713),
          data =>{
              ps.setString(1,data._1)
              ps.setString(2,data._2)
              //执行SQL
              ps.executeUpdate()
          }
      )

    }catch{
      case e1:Exception => println("Some Error Happend!" + e1.getMessage)
    }finally {
      //释放资源
      if(ps != null) ps.close()
      if(conn != null) conn.close()
    }
  }

}
















