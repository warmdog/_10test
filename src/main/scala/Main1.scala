import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main1 {
  val file = "/app/user/data/deeplearning/results/triangleVertices/morethan60and10/part-00000"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark GraphXXX").getOrCreate()
    val accum0 = spark.sparkContext.longAccumulator("My Accumulator0")
    val accum1= spark.sparkContext.longAccumulator("My Accumulator1")
    val accum2 = spark.sparkContext.longAccumulator("My Accumulator2")
    val accum3 = spark.sparkContext.longAccumulator("My Accumulator3")
    val blackDataSet: Dataset[Row] = spark.sql("select phone from blacklist3.black_type_list_new where createdate<'2017-06-07 00:00:00' and phone !=''")
    val rdd = spark.sparkContext.textFile(file)
    val black =blackDataSet.rdd.map(x =>{
      val id = x.getAs[String](0).toLong
      id
    })

    val arrayBlack = black.collect()
    val broadcast = spark.sparkContext.broadcast(arrayBlack)

    val sortedRdd = rdd.map(x =>{
      val parts = x.split('\t').map(x =>x.toLong).toList.sortWith(_>_)
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
        (0,0,0)
      }
    })
    newRdd.foreach(x =>{
      var i =0
      if (broadcast.value.contains(x._1)) i = i+1
      if(broadcast.value.contains(x._2)) i = i +1
      if (broadcast.value.contains(x._3)) i = i+1
      if(i ==0) accum0.add(1)
      if(i ==1) accum1.add(1)
      if(i ==2) accum2.add(1)
      if(i ==3) accum3.add(1)
    })

  }
}
