import org.apache.spark.sql.SparkSession

object TriAnalysis {
  val file = "/app/user/data/deeplearning/results/triangleVertices/withV5Variance2/part-00000"
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Spark GraphXXX").getOrCreate()
    val rdd = spark.sparkContext.textFile(file)
    val accum0 = spark.sparkContext.longAccumulator("My Accumulator0")
    val accum1= spark.sparkContext.longAccumulator("My Accumulator1")
    val accum2 = spark.sparkContext.longAccumulator("My Accumulator2")
    val accum3 = spark.sparkContext.longAccumulator("My Accumulator3")
    val regex = "[(|)]".r
    rdd.foreach(x =>{
      val parts = x.split(",")
      var sum  =0
      val a = parts(1).toDouble
      val b = parts(3).toDouble
      val c = parts(5).toDouble
      if(Math.abs(a-b)>0.1) sum = sum+1
      if(Math.abs(c-b)>0.1) sum = sum+1
      if(Math.abs(a-c)>0.1) sum = sum+1
      if(sum == 0) { accum0.add(1)}
      else if(sum ==1) {accum1.add(1)}
      else if(sum ==2) {accum2.add(1)}
      else{
        accum3.add(1)
      }
    })
  }
}
