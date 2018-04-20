
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main_filter {
  val file  = "/app/user/data/deeplearning/results/triangleVertices/v5_score.dat"
  val tri = "/app/user/data/deeplearning/results/triangleVertices/count/part-00000"
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Spark GraphXXX").getOrCreate()
    val uuidRdd = spark.sql(s"select uuid, phone_no from xyqb.`user` where uuid!='' and phone_no !=''  ").rdd.map(x => {
      val uuid = x.getAs[String](0).trim
      val phone = x.getAs[String](1).trim.toLong
      (uuid, phone)
    })



    val triRDD =spark.sparkContext.textFile(tri).repartition(100)
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



    val common  = v5Rdd.leftOuterJoin(uuidRdd).map(x =>{
      val phone = x._2._2.getOrElse(0L)
      val score = x._2._1
      (phone,score)
    }).collect().toMap

    println(common.size)

    val broadcast = spark.sparkContext.broadcast(common)
    val regex = "[0-9]".r
    val triScore =triRDD.map(x =>{
      val parts = x.split(",")
      val part1 =(regex findAllIn parts(0)).mkString("").toLong
      val part2 =(regex findAllIn parts(1)).mkString("").toLong
      val part3 = (regex findAllIn parts(2)).mkString("").toLong
      val num = (regex findAllIn parts(3)).mkString("")
      val value1 = broadcast.value.getOrElse(part1,0.0)
      val value2 = broadcast.value.getOrElse(part2,0.0)
      val value3 = broadcast.value.getOrElse(part3,0.0)
      val ava = (value1+value2+value3)/3
      val score = ((value1-ava)*(value1-ava) +(value2-ava)*(value2-ava) +(value3-ava)*(value3-ava))/3
      (part1,value1,part2,value2,value3,part3,num,score)
    }).repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/triangleVertices/withV5Variance")


    val regex1 = "[0-9|.]".r
    val rdd = spark.sparkContext.textFile("/app/user/data/deeplearning/results/triangleVertices/withV5Variance/part-00000")
    val file  = rdd.map(x =>{
      val parts = x.split(",")
      val part1 = (regex1 findAllIn parts(0)).mkString("").toLong
      val part2 = (regex1 findAllIn parts(1)).mkString("").toDouble
      val part3 = (regex1 findAllIn parts(2)).mkString("").toLong
      val part4 = (regex1 findAllIn parts(3)).mkString("").toDouble
      val part5 = (regex1 findAllIn parts(4)).mkString("").toDouble
      val part6 = (regex1 findAllIn parts(5)).mkString("").toLong
      //val part7 = (regex1 findAllIn parts(7)).mkString("").toDouble
      val aver = (part2+part4+part5)/3.0
      val part7 = ((part2-aver)*(part2-aver)+ (part4-aver)*(part4-aver) +(part5-aver)*(part5-aver))/3.0
      (part1,part2,part3,part4,part6,part5,part7)
    }).filter(x =>x._2>0.0 &&x._4>0.0 && x._6>0.0).repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/triangleVertices/withV5Variance2")


  }
}
