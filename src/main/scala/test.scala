import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}

object test {
  def main(args: Array[String]): Unit = {
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    /*
    1.0  2.0
   3.0  4.0
   5.0  6.0
    */
    //val res = 10 * dm
    val dm1: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 4.0, 2.0, 4.0, 6.0))
    //println(10 * dm)
    val user_tag_matrixNet2_0 = Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0).toList
    //val matrixNet1 = new CoordinateMatrix( MatrixEntry(10,20,40))
    //val matrixNet2 = new CoordinateMatrix(user_tag_matrixNet2_0.map(x => MatrixEntry(x, x._2, x._3)))
    //val matrix_link = matrixNet1.toBlockMatrix(2048, 2048).multiply(matrixNet2.toBlockMatrix(2048, 2048))
    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}
    import scala.collection.mutable
    import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

        //    val ss: (String, String) = ("wdw","dwdw")
        //    val sss = "dwdw,d,dwdwa"
        //    val a = sss.split(",")
        //    print(a(2)+a(0))
        //val aaa =  new mutable.HashMap[]
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf()
        val tes = Map[String,Int]()
        //import sparkSession.implicits._
        val sc = new SparkContext("local","wordcount",conf)
        println("test reduceByKey")
        val a = sc.parallelize(List((1,2),(1,3),(3,4),(3,6)))
        val b =a.map(v=>(v._2,v._1)).reduceByKey((x,y) => y).map(z =>z._2)
        val c =a.map(v=>(v._2,v._1)).reduceByKey((x,y) => x).map(z =>z._1)
        b.foreach(println(_))
        c.foreach(println(_))
        val users: RDD[(VertexId, (String, String))] =
          sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
            (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),(1L, ("jgonzal", "prof"))),2)
        //Create an RDD for edges
        val relationships: RDD[Edge[String]] =
          sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(7L, 3L, "collab"),   Edge(5, 7L, "pi"), Edge(5L, 3L, "advisor"), Edge(3L, 5L, "pi"),
            Edge(2L, 5L, "colleague"), Edge(7L, 5L, "pi"),Edge(1L, 2L, "pi")),2)
        // Define a default user in case there are relationship with missing user
        val defaultUser = ("John Doe", "Missing")
        val value: Graph[(String, String), String] = Graph(users, relationships)
        val value2: Graph[(String, String), String] = value
        val graph = value2
        println("remove 5 subgraph")
        val value5: Graph[(String, String), String] = graph.subgraph(vpred = (id, attr) => attr._1 != "istoica")
        val validGraph1 = value5
        validGraph1.degrees.collect.foreach(println(_))
        validGraph1.triplets.map(
          triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
        ).collect.foreach(println(_))
        // Build the initial Graph

        //graph.vertices.filter(x =>x._2!=null).foreach(println(_))
        graph.edges.foreach(println(_))
        // val graph: Graph[(String, String), String] // Constructed from above
        // Count all users which are postdocs
        //graph.mapVertices(case (id,_) =>id)
        //graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
        // Count all the edges where src > dst
        graph.edges.filter(e => e.srcId > e.dstId).count
        //triplets 就是包含多个： 两个顶点的信息triplet.srcAttr，triplet.dstAttr 和边的信息 triplet.attr
        val facts:RDD[String]=
          graph.triplets.map(triplet =>
            triplet.srcAttr._1+" " + triplet.srcAttr._2+ " is the " + triplet.attr + " of " + triplet.dstAttr._1)
        //facts.persist()
        facts.collect().foreach(println(_))
        //graph
        // inDegrees 入度 Tuple2[VertexId, VD] 统计入度大于等于2的个数
        println(graph.inDegrees.filter(x =>x._2==2 ).collect().length)
        val value1: RDD[(VertexId, Int)] = graph.degrees.filter(x => x._2 == 2).sortByKey(false)

        // 统计每个节点三角形个数  example:graph.triangleCount().vertices.collect()
        //                      res0: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((1,1), (3,1), (2,1))
        val count: VertexRDD[Int] = graph.triangleCount().vertices.filter(x => x._2>=1)
        count.cache()
        // 下面2句子导致  WARN Joining two VertexPartitions with different indexes is slow. 原因暂时觉得是 vertexPartitions 应该分区处理，
        // 不应该一起拉回
        count.foreach(println(_))
        //count.collect()
        //创建新的 graph 在原有基础上
        println( "new graph:")
        //val newGraph1 = graph.mapEdges(x => x.attr*2)
        val newGraph2 = graph.mapVertices{ case (id,_) => 1.0}
        //加大括号 是对value赋值结果(2,(2,2,2,2))
        val newGraph3  =  graph.mapVertices((vid,data) =>{
          (vid.toString,vid,vid,vid)
        })
        newGraph3.vertices.foreach(println(_))
        println("MAP vertices  graph.mapVertices{case (id,_) => 1.0}")
        newGraph2.vertices.foreach(println(_))
        val value6: Graph[VertexId, String] = graph.connectedComponents()
        // Run Connected Components 添加条件选出子图 步骤
        val ccGraph = value6 // No longer contains missing field
        // Remove missing vertices as well as the edges to connected to them
        //
        val validGraph = graph.subgraph(vpred = (id, attr) => attr._1 !="istoica" && attr._1 !="999")
        // Restrict the answer to the valid subgraph
        println("subgraph: ")
        //graph.connectedComponents() 获取联系的最小值(1,1) 单独一个点
        //有联系 都是3
        //(7,3)(3,3)
        VertexRDD
        //输出有几个图
        validGraph.connectedComponents().vertices.foreach(println(_))
        println("map reduce ")
        val numberofGraph: RDD[(String, Int)] = validGraph.degrees.mapPartitions(x => {
          var map = mutable.HashMap[String, Int]()
          while (x.hasNext) {
            val cul = x.next()._2.toString
            if (!map.contains(cul)) {
              map.put(cul, 1)
            }
            else {
              val num = map.get(cul).get + 1
              map.put(cul, num)
            }
          }
          map.iterator
        }).repartition(4).reduceByKey((x, y) => x + y).repartition(1)
        //numberofGraph.saveAsTextFile("d:/Users/Desktop/test1")
        println("connectedComponents vertices foreach")
        validGraph.connectedComponents().vertices.foreach(x =>{
          println(x._2)
        })
        println("xxxxxxxxxxxxx")
        //    val l: VertexId = validGraph.connectedComponents().vertices.mapPartitions(x => {
        //      val res = List[VertexId]()
        //      while (x.hasNext) {
        //        val tmp = x.next()._2
        //
        //        res.::(tmp)
        //      }
        //      res.iterator
        //    }).distinct().count()
        val degreeRDD = validGraph.degrees
        //计算 有几个图
        // 一个rdd中  transform 不能引用其他rdd
        //    val l: VertexId = validGraph.connectedComponents().vertices.map(x =>{
        //        if (degreeRDD.filter(y=>y==x._2).isEmpty()) { 0L}
        //        else { x._2}
        //    }).distinct().count()
        // println(l)
        println("xxxxxxxxxxxxxxxxx")
        println(validGraph.connectedComponents().vertices.map(x =>x._2).repartition(2).distinct().count())
        println("xxxxxxx")
        val value4: Graph[VertexId, String] = ccGraph.mask(validGraph)
        val validCCGraph = value4
        //println(graph.vertices.collect().mkString(" "))
        println(validCCGraph.vertices.collect().mkString(" "))
        // joinVertices 链接图
        println("JoinVertices:=====")
        val nonUniqueCosts: RDD[(VertexId, Int)] = graph.inDegrees
        val uniqueCosts: VertexRDD[Int] =
          graph.vertices.aggregateUsingIndex(nonUniqueCosts, (a,b) => a + b)
        val joinedGraph = graph.joinVertices(uniqueCosts)(
          (id, oldCost, extraCost) => oldCost)
        joinedGraph.inDegrees.foreach(println(_))
        println(" outerjoinVertices outdegree :=====")
        graph.outDegrees.foreach(println(_))
        val outDegrees: VertexRDD[Int] = graph.degrees
        val value3: Graph[Int, String] = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
          outDegOpt match {
            case Some(outDeg) => outDeg
            case None => 0 // No outDegree means zero outDegree
          }
        }
        val degreeGraph = value3
        //graph.filter()
        //graph.degrees
        //graph.vertices.leftJoin()
        val ss= sc.collectionAccumulator[Long]("sss")
        val accumulator = sc.longAccumulator("Sum")
        val sum = sc.longAccumulator("Count")
        degreeGraph.vertices.foreach(println(_))
        println("分区数量：")
        var max = 0;
        var min = Integer.MAX_VALUE
        println(graph.vertices.partitions.size)
        graph.degrees.coalesce(4,shuffle = true).foreachPartition(x =>{
          //val x1: Iterator[(VertexId, Int)] = x
          x.foreach( y=>{
            accumulator.add(y._2)
            sum.add(1)
            ss.add(y._2.toLong)
            if(y._2 >max) max =y._2
            if(y._2 < min) {min =y._2}
          })
          println(s"xxxxxxxxxx max'${max}'")
          println(s"xxxxxxxxxxx  min'${min}'")
        })
        println(accumulator.value)
        ss.value.contains(2)
        val l: VertexId = validGraph.connectedComponents().vertices.map(x =>{
          if (ss.value.contains(2)) { 0L}
          else { x._2}
        }).distinct().count()
        println(l)
        //Graph.fromEdges()
        //不可改变变量 这种方法不对
        //    graph.outDegrees.map(a =>
        //      a._2 match {
        //         case  Some(b :Int) => b
        //         case None =>0
        //       }
        //
        //    )

      }



}
