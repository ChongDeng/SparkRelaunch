package demo

import java.util.Properties
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//作用：读取本地一个文件, 生成对应 DataFrame，注册表
object Demo1 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\D_study\\hadoop_winutils\\")

    //创建SparkSession
    val spark = SparkSession.builder().master("local").appName("My Demo 1").getOrCreate()

    //从指定的文件中读取数据，生成对应的RDD
    val personRDD = spark.sparkContext.textFile("C:\\D_study\\hadoop&spark\\student.txt")
        .map(_.split(" "))

    //创建schema ，通过StructType
    val schema = StructType(
      List(
        StructField("personID",IntegerType,true),
        StructField("personName",StringType,true),
        StructField("personAge",IntegerType,true)
      )
    )

    //将RDD映射到Row RDD 行的数据上
    val rowRDD = personRDD.map(p => Row(p(0).toInt,p(1).trim,p(2).toInt))

    //生成DataFrame
    val personDF = spark.createDataFrame(rowRDD,schema)
    //将DF注册成表
    personDF.createOrReplaceTempView("myperson")
    //执行SQL
    val result = spark.sql("select * from myperson")

    //显示
    //result.show()

    //将结果保存到oracle中
    val props = new Properties()
    props.setProperty("user","scott")
    props.setProperty("password","tiger")

    result.write.jdbc("jdbc:oracle:thin:@192.168.88.101:1521/orcl.example.com","scott.myperson",props)

    //如果表已经存在,append的方式数据
    //result.write.mode("append").jdbc("jdbc:oracle:thin:@192.168.88.101:1521/orcl.example.com","scott.myperson",props)
    //停止spark context
    spark.stop()
  }
}
















