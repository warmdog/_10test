
import TextProcess.v5file
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import scala.collection.mutable.ListBuffer

object App_Process {
  val v5file = "/app/user/data/deeplearning/results/TextProcess/train_data_with_v5.csv"
  val android = "/app/user/data/deeplearning/results/TextProcess/android-7-12.csv"
  case class Params(times :Long= 1000*60*60*2)
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Parse"){
      head("Parse")
      opt[Long]("times").text(s"times: ${defaultParams.times}") .action((x, c) => c.copy(times = x))
    }
    val config = parser.parse(args, defaultParams)
    val spark = SparkSession.builder().appName("Text Parse").getOrCreate()
    import spark.implicits._

    val v5F = spark.read.format("csv").option("header","true").load(v5file)
    val androidData = spark.read.format("csv").option("delimiter", "\t").option("header","true").load(android)
    //seq 可以避免重复的列名
    val v5 = androidData.join(v5F,Seq("uuid","applied_at"))
    val v5_train = v5.filter($"applied_at" >"2017-07-00 00:00:00" && $"applied_at" <"2017-11-00 00:00:00")

      val v5_predict1 = v5.filter($"applied_at" >"2017-11-00 00:00:00" && $"applied_at" <"2017-12-00 00:00:00").rdd.map(x =>{
      val uuid = x.getString(0)
      val applied_at = x.getString(1)
      val resp =x.getString(4)
      (uuid,(applied_at,resp))
    }).filter(x => !x._2._2.isEmpty &&( x._2._2 != "NaN"))

    val v5_predict2 = v5.filter($"applied_at" >"2017-12-00 00:00:00" && $"applied_at" <"2018-01-00 00:00:00").rdd.map(x =>{
      val uuid = x.getString(0)
      val applied_at = x.getString(1)
      val resp =x.getString(4)
      (uuid,(applied_at,resp))
    }).filter(x => !x._2._2.isEmpty &&( x._2._2 != "NaN"))

    val sms = spark.sql(s"select  userid, applist,createtime from hbase.user_app_list ").rdd

    val smsRow = sms.map(x =>{
      try{
        val userid = x.getAs[String](0).trim()
        val smscontent = x.getAs[String](1).trim()
        val time = x.getAs[String](2).trim()
        (userid,(smscontent,time))
      }catch{
        case e => ("0",("0","0"))
      }}).cache()

    val v5_train_rdd =v5_train.rdd.map(x =>{
      val uuid = x.getString(0)
      val applied_at = x.getString(1)
      val resp =x.getString(4)
      (uuid,(applied_at,resp))
    })filter(x => !x._2._2.isEmpty &&( x._2._2 != "NaN"))

    import java.text.SimpleDateFormat
    object Time{

      def formatDate(s:String):Long = {
        val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        dateFormat.parse(s).getTime()
      }

    }

    val V5res =v5_train_rdd.join(smsRow).filter(x =>{
      if((Time.formatDate(x._2._2._2) - Time.formatDate(x._2._1._1))<config.get.times){
        true
      }else{
        false
      }
    }).groupByKey().map(x =>{
      var all = new ListBuffer[String]()
      var applied_at =""
      var res = ""
      var resp = ""
      for ( s <- x._2){
        val parts = s._2._1.split("##")
        resp = s._1._2
        applied_at = s._1._1
        parts.foreach(y =>{
          val  is = y.trim.replaceAll("(\0|\\s*|\r|\n)", "").replaceAll("Null","")
          if(!is.isEmpty){
            all += is
          }
        })
      }
      all.distinct.toList.foreach(y =>{
        res = y + " " + res
      })
      res = res + "\t__label__" + resp
      x._1 + "," + applied_at + "," + res
    })

    val res_predict1 = v5_predict1.join(smsRow).filter(x =>{
      if((Time.formatDate(x._2._2._2) - Time.formatDate(x._2._1._1))<config.get.times){
        true
      }else{
        false
      }
    }).groupByKey().map(x =>{
      var all = new ListBuffer[String]()
      var applied_at =""
      var res = ""
      var resp = ""
      for ( s <- x._2){
        val parts = s._2._1.split("##")
        resp = s._1._2
        applied_at = s._1._1
        parts.foreach(y =>{
          val  is = y.trim.replaceAll("(\0|\\s*|\r|\n)", "").replaceAll("Null","")
          if(!is.isEmpty){
            all += is
          }
        })
      }
      all.distinct.toList.foreach(y =>{
        res = y + " " + res
      })
      res = res + "\t__label__" + resp
      x._1 + "," + applied_at + "," + res
    })

    val res_predict2 = v5_predict2.join(smsRow).filter(x =>{
      if((Time.formatDate(x._2._2._2) - Time.formatDate(x._2._1._1))<config.get.times){
        true
      }else{
        false
      }
    }).groupByKey().map(x =>{
      var all = new ListBuffer[String]()
      var applied_at =""
      var res = ""
      var resp = ""
      for ( s <- x._2){
        val parts = s._2._1.split("##")
        resp = s._1._2
        applied_at = s._1._1
        parts.foreach(y =>{
          val  is = y.trim.replaceAll("(\0|\\s*|\r|\n)", "").replaceAll("Null","")
          if(!is.isEmpty){
            all += is
          }
        })
      }
      all.distinct.toList.foreach(y =>{
        res = y + " " + res
      })
      res = res + "\t__label__" + resp
      x._1 + "," + applied_at + "," + res
    })

    V5res.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/AppProcess/train")
    res_predict1.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/AppProcess/predict1")
    res_predict2.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/AppProcess/predict2")




  }
}
