import org.apache.spark.graphx.{Edge, Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Expander {
  val filePath = ""
  def main(args: Array[String]): Unit = {

  }
  def loadGraph(sparkSession: SparkSession):Graph[Int,Int] ={
    val fileRDD: RDD[String] = sparkSession.sparkContext.textFile(filePath)
    val edgeRDD = fileRDD.map(x =>{
      val item= x.trim.split("\\s+")
      Edge(item(0).toLong,item(1).toLong,item(3).toInt)
    })
    val blackDataSet: Dataset[Row] = sparkSession.sql("select phone from blacklist.black_type_list_new")
    val blackRDD: RDD[Row] = blackDataSet.rdd
    val regex = "[0-9]".r
    val vertexRDD: RDD[(VertexId, Int)] = blackRDD.map(x => {
      val s = x.toString().trim
      //hive 获取多行数据方法 val value = x.getAs[String](1)
      //val num = x.getAs[Long](2)
      ((regex findAllIn s).mkString("").toLong, Integer.valueOf(1))
    })
    val value: Graph[PartitionID, PartitionID] = Graph.fromEdges(edgeRDD, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel =
      StorageLevel.MEMORY_AND_DISK_SER)
    //Graph(vertexRDD,edgeRDD)
    val result: Graph[PartitionID, PartitionID] = value.outerJoinVertices(vertexRDD) { (id, oldAttr, outDegOpt) =>
      //      outDegOpt match {
      //        case Some(outDegOpt) => outDegOpt
      //        case None =>oldAttr
      //      }
      outDegOpt.getOrElse(0)
    }
    result
  }
}
