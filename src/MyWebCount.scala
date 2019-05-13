import org.apache.spark.{SparkConf, SparkContext}

//需求: 求出访问量最高的两个网页
object MyWebCount {
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

        //返回一个元组:  (head.jsp,1)
        (line3,1)
      }
    )

    //按照key进行聚合操作:
    val rdd2 = rdd1.reduceByKey( _ + _)

    //按照rdd2的value进行排序 :  (web.jsp,3)
    val rdd3 = rdd2.sortBy(_._2,false)

    //输出
    println(rdd3.take(2).toBuffer)

    //停止sc
    sc.stop()
  }
}



















