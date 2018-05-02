import TriangleMain.{dataFile, file, triangle}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main {
  val dataFile = "/app/user/data/deeplearning/results/triangleVertices/input.txt"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark GraphXXX").getOrCreate()
    val data = spark.sparkContext.textFile(dataFile).map(x => {
      val parts = x.split("\\s+")
      (parts(0),parts(1))
    }).collect()
    val times =data.apply(0)._1.toInt
    val number = data.apply(0)._2.toInt
    val fileRDD: RDD[Row] = spark.sql("select phone,receiverphone,total_num,durations_time from deeplearning.t_user_relation_phone where dt='20170606' and durations_time >'"+times+"' and total_num >'"+number+"'").rdd
    val edgeRDD=fileRDD.map(x =>{
      val phone = x.getAs[String](0).toLong
      val receiverphone = x.getAs[String](1).toLong
      val number = x.getAs[Long](2)
      val durations_time = x.getAs[Long](3)

      (phone,receiverphone,number,durations_time)

    }).map(x =>{
      Edge(x._1,x._2,x._3)
    }).filter(x =>x.dstId!=x.srcId)
    // 黑名单最终日期  2017-10-19 13:58:45
    // 通话记录最终日期 2017-10-08
    val blackDataSet: Dataset[Row] = spark.sql("select phone from blacklist3.black_type_list_new where createdate<'2017-06-07 00:00:00'")
    val xyqbDataSet:Dataset[Row] = spark.sql("select phone_id from xyqb.t_user where created_at<'2017-06-07 00:00:00'")
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
    //println(s"blackRDD: ${vertexRDD1.count()}")
    //println(s"xqybRDDcount: ${vertexRDD1.top(10).foreach(println(_))}" )
    // 1 黑名单 2 xyqb用户
    val value: Graph[Int, Long] = Graph.fromEdges(edgeRDD, 0)
    //Graph(vertexRDD,edgeRDD)
    val graph = value.outerJoinVertices(vertexRDD1) { (id, oldAttr, outDegOpt) =>
      //      outDegOpt match {
      //        case Some(outDegOpt) => outDegOpt
      //        case None =>oldAttr
      //      }
      outDegOpt.getOrElse(0)
    }
    val black1  = graph.subgraph(vpred = (vertexId, value) => (value== 1 || value==2))
    val result =black1
    println("new Graph:")
    result.vertices.top(10).foreach(println(_))

    val tri =triangle(black1)

    val accum0 = spark.sparkContext.longAccumulator("My Accumulator0")
    val accum1= spark.sparkContext.longAccumulator("My Accumulator1")
    val accum2 = spark.sparkContext.longAccumulator("My Accumulator2")
    val accum3 = spark.sparkContext.longAccumulator("My Accumulator3")
    val blackDataSet1: Dataset[Row] = spark.sql(s"select phone from blacklist3.black_type_list_new where createdate<'2017-06-07 00:00:00' and phone !=''")
    val rdd = tri
    val black =blackDataSet1.rdd.map(x =>{
      val id = x.getAs[String](0).toLong
      id
    })

    val arrayBlack = black.collect()
    val broadcast1 = spark.sparkContext.broadcast(arrayBlack)

    val sortedRdd = rdd.repartition(300).map(x =>{
      val parts = x.split("\\s+").map(x =>x.toLong).toList.sortWith(_>_)
      var s =""
      for (ss <- parts){
        s = s+ss;
      }
      val res =s;
      res
    }).distinct()

    val newRdd =sortedRdd.map( x =>{
      if(x.length ==57){

        val parts1 = x.substring(0,19).toLong
        val parts2 = x.substring(19,38).toLong
        val parts3 = x.substring(38,57).toLong
        (parts1,parts2,parts3)

      }else{
        (0L,0L,0L)
      }
    })
    val newRdd1 =newRdd.map(x =>{
      var i =0
      var res =0
      if (broadcast1.value.contains(x._1)) i = i+1
      if(broadcast1.value.contains(x._2)) i = i +1
      if (broadcast1.value.contains(x._3)) i = i+1
      if(i ==0){ accum0.add(1)
        res=0}
      if(i ==1) {
        accum1.add(1)
        res =1
      }
      if(i ==2) {
        res =2
        accum2.add(1)
      }
      if(i ==3) {
        res=3
        accum3.add(1)
      }
      (x._1,x._2,x._3,res)
    })

    val v5 = spark.sparkContext.textFile("/app/user/data/deeplearning/results/triangleVertices/v5_score.dat")
    val head = v5.first()
    val v5Rdd =  v5.filter(row =>row!=head).map(x =>{
      val parts = x.split("\\s+")
      if(parts.length>=2){
        val uuid =parts(0).trim
        val score = parts(1).toDouble
        (uuid,score)
      }else{
        ("",0.0)
      }
    })

    val uuidRdd = spark.sql(s"select uuid, phone_no from xyqb.`user` where uuid!='' and phone_no !=''  ").rdd.map(x => {
      val uuid = x.getAs[String](0).trim
      val phone = x.getAs[String](1).trim.toLong
      (uuid, phone)
    })

    val common  = v5Rdd.leftOuterJoin(uuidRdd).map(x =>{
      val phone = x._2._2.getOrElse(0L)
      val score = x._2._1
      (phone,score)
    }).collect().toMap

    val broadcast = spark.sparkContext.broadcast(common)

    val triScore =newRdd1.map(x =>{

      val part1 = x._1
      val part2 = x._2
      val part3 = x._3
      val num = x._4
      val value1 = broadcast.value.getOrElse(part1,0.0)
      val value2 = broadcast.value.getOrElse(part2,0.0)
      val value3 = broadcast.value.getOrElse(part3,0.0)
      val ava = (value1+value2+value3)/3
      val score = ((value1-ava)*(value1-ava) +(value2-ava)*(value2-ava) +(value3-ava)*(value3-ava))/3
      (part1,value1,part2,value2,part3,value3,num,score)
    }).repartition(1).filter(x =>x._2>0.0 &&x._4>0.0 && x._6>0.0).saveAsTextFile("/app/user/data/deeplearning/results/triangleVertices/withV5Variance3")
  }

  def triangle(graph: Graph[Int,Long]):RDD[String]={
    val results =NewTriangleCount.run(graph).vertices.map(x =>{
      val sb = new StringBuilder()
      try{
        val res:List[Array[String]] = x._2.toList
        var i =0
        for (n<-res){
          //x._2.map(_.mkString("\t")).mkString("\n")
          if(i==res.length-1){

            sb.append(x._1 + "\t" + n.mkString("\t"))
          }else {
            sb.append(x._1 + "\t" + n.mkString("\t")+"\n")
          }
          i= i+1
        }
      }catch {
        case e:NullPointerException =>x._1 +"\t" +null
      }
      sb.toString()
    }).filter(x =>x.length!=0)//.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/triangleVertices/morethan60and10")

    val results1 =NewTriangleCount.run(graph).vertices.flatMap(x =>{
      var list = List[String]()
      try {
        val res: List[Array[String]] = x._2.toList
        var i = 0
        for (n <- res) {

          //x._2.map(_.mkString("\t")).mkString("\n")
          list = (x._1 + "  " + n.mkString("  "))::list
        }
      }catch {
        case e:NullPointerException =>x._1 +"\t" +null
      }
      list
    })
  results1
  }
}
