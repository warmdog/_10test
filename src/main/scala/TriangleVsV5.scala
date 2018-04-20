import Main.{dataFile, triangle}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * author zs
  * time 2018-4-8
  * 保存在三角形中的 节点为1 不在为0
  */

object TriangleVsV5 {
  val desFile = "/app/user/data/deeplearning/results/triangleVertices/TriangleVsV5"
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark GraphXXX").getOrCreate()
    val graph = (new iniGraph(spark)).getGraph()
    val tri :RDD[String] =triangle(graph)

    val uuidRdd = spark.sql(s"select uuid, phone_no from xyqb.`user` where uuid!='' and phone_no !=''  ").rdd.map(x => {
      val uuid = x.getAs[String](0).trim
      val phone = x.getAs[String](1).trim.toLong
      (phone, uuid)
    })

    val triCount = graph.triangleCount().vertices.map(x =>{
      (x._1.toLong,x._2)
    })

    val joinRDD = triCount.join(uuidRdd).filter(x =>x._2._1>0).map(x =>{
      val uuid = x._2._2
      val phone = x._1
      (uuid+","+x._2._1+","+1)
    })

    joinRDD.repartition(1).saveAsTextFile(desFile)

   

  }

}
