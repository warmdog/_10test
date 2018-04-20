
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object Parse {
  case class Params(times :Long= 1000*60*60*2)
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Parse"){
      head("Parse")
      opt[Long]("times").text(s"times: ${defaultParams.times}") .action((x, c) => c.copy(times = x))
    }
    val config = parser.parse(args, defaultParams)
    val timess =config.get.times
    val test = List(timess,timess)
    val spark = SparkSession.builder().appName("Text Parse").getOrCreate()
    val accum0 = spark.sparkContext.longAccumulator("My Accumulator0")
    val rdd =spark.sparkContext.parallelize(test)
    rdd.foreach(x =>{
      accum0.add(x)
    }
    )
  }
}
