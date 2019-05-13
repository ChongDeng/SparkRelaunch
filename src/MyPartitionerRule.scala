import org.apache.spark.Partitioner

import scala.collection.mutable

//自定义的分区规则: key : jsp文件名称   head.jsp , oracle.jsp
class MyPartitionerRule(allJSPName: Array[String]) extends Partitioner{
  //定义一个Map集合,保存分区的规则:  head.jsp ---> 1     oracle.jsp ---> 2
  val partitionMap = new mutable.HashMap[String,Int]()

  //生成分区的规则
  var partID = 0
  for(jspName <- allJSPName){
    partitionMap.put(jspName,partID)  // head.jsp 1
    partID += 1
  }

  //分区的个数
  override def numPartitions: Int = partitionMap.size

  override def getPartition(key: Any): Int = {
    //根据JSP的文件名称返回对应的PartID
    partitionMap.getOrElse(key.toString,0)
  }
}
