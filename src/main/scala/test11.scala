import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test11 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    val tes = Map[String,Int]()
    //import sparkSession.implicits._
    val sc = new SparkContext("local","wordcount",conf)
    var s  =List("你好兄弟是个上地的大傻比真的会分次吗" ,".toString")
    sc.parallelize(s).map(x =>{
      var str = if (x.length>0) new JiebaSegmenter().sentenceProcess(x)
      str
      11+str.toString.replaceAll(","," ")+"dwdwdwd"
    }).repartition(1).saveAsTextFile("E:\\test1")
     // #if( s.contains(x)  )  print(ss)

  }
}
