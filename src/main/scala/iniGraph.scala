import Main.dataFile
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class iniGraph(sparkSession: SparkSession) {

  def getGraph() : Graph[Int, Long] ={
    val spark = sparkSession
    val data = spark.sparkContext.textFile(dataFile).map(x => {
      val parts = x.split("\\s+")
      (parts(0),parts(1))
    }).collect()
    val times =data.apply(0)._1.toInt
    val number = data.apply(0)._2.toInt
    val fileRDD: RDD[Row] = spark.sql("select phone,receiverphone,total_num,durations_time from deeplearning.t_user_relation_phone where dt='20171008'  and durations_time >'"+times+"' and total_num >'"+number+"'").rdd
    val edgeRDD=fileRDD.map(x =>{
      val phone = x.getAs[String](0).toLong
      val receiverphone = x.getAs[String](1).toLong
      val number = x.getAs[Long](2)
      val durations_time = x.getAs[Long](3)
      (phone,receiverphone,number,durations_time)
    }).map(x =>{
      Edge(x._1,x._2,x._3)
    }).filter(x =>x.dstId!=x.srcId)

    val blackDataSet: Dataset[Row] = spark.sql("select phone from blacklist3.black_type_list_new where createdate<'2017-10-09 00:00:00'")
    val xyqbDataSet:Dataset[Row] = spark.sql("select phone_no from xyqb.`user` where created_at<'2017-10-09 00:00:00'")
    val blackRDD: RDD[Row] = blackDataSet.rdd
    //println(s"blackRDD: ${blackRDD.count()}")
    val xyqbRDD = xyqbDataSet.rdd
    val regex = "[0-9]".r

    val vertexRDD= blackRDD.map(x => {
      val s = x.toString().trim
      //hive 获取多行数据方法 val value = x.getAs[String](0)
      //val num = x.getAs[Long](1)
      val part =(regex findAllIn s).mkString("")
      if(part !="" && part!= null){
        (part.toLong,1)
      }else{
        (0L,1)
      }
    })

    val vertexRDD1: RDD[(VertexId, Int)] = xyqbRDD.map(x => {
      val s = x.toString()
      //hive 获取多行数据方法 val value = x.getAs[String](1)
      //val num = x.getAs[Long](2)
      ((regex findAllIn s).mkString("").toLong, 1)
    }).leftOuterJoin(vertexRDD).map(x =>{
      (x._1,x._2._2.getOrElse(2))
    })

    // 1 黑名单 2 xyqb用户
    val value: Graph[Int, Long] = Graph.fromEdges(edgeRDD, 0)

    val graph = value.outerJoinVertices(vertexRDD1) { (id, oldAttr, outDegOpt) =>
      //      outDegOpt match {
      //        case Some(outDegOpt) => outDegOpt
      //        case None =>oldAttr
      //      }
      outDegOpt.getOrElse(0)
    }
    val black1  = graph.subgraph(vpred = (vertexId, value) => (value== 1 || value==2))
    black1
  }
}
