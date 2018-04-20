

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser

object TextProcess {
  //__label__affairs
  val v5file = "/app/user/data/deeplearning/results/TextProcess/train_data_with_v5.csv"
  case class Params(times :Long= 1000*60*60*2)
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TextProcess"){
      head("TextProcess")
      opt[Long]("times").text(s"times: ${defaultParams.times}") .action((x, c) => c.copy(times = x))
    }
    val config = parser.parse(args, defaultParams)
    val spark = SparkSession.builder().appName("Text Process").getOrCreate()
    val v5 = spark.read.format("csv").option("header","true").load(v5file)
    //v5.columns
    import spark.implicits._

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
    //val smsOld = spark.sql(s"select  userid, smscontent,createtime from deeplearning.user_sms_info ").rdd
    val sms = spark.sql(s"select  userid, smscontent,createtime from hbase.user_sms_info ").rdd
    val schemaString = "uuid  appled_at  smscontent"
    //val sms = smsNew.union(smsOld)

    val smsRow = sms.map(x =>{
      try{
        val userid = x.getAs[String](0).trim()
        val smscontent = x.getAs[String](1).trim()
        val time = x.getAs[String](2).trim()
        (userid,(smscontent,time))
      }catch{
        case e => ("0",("0","0"))
      }})

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
      if((Time.formatDate(x._2._2._2) - Time.formatDate(x._2._1._1))<0){
        true
      }else{
        false
      }
    }).groupByKey().map(x =>{
      var s =""
      var applied_at =""
      var resp = ""
      for(y <- x._2){
        s =s + y._2._1.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]", "")
        applied_at = y._1._1
        resp = y._1._2
      }
      var str = if (s.length>0) new JiebaSegmenter().sentenceProcess(s)
      val fenci = str.toString.replaceAll(","," ")
      val fenciLabel = fenci + "\t__label__" + resp
      x._1 + "," + applied_at + ", "+fenciLabel
      //fenciLabel
    })

    val res_predict1 =v5_predict1.join(smsRow).filter(x =>{
      if((Time.formatDate(x._2._2._2) - Time.formatDate(x._2._1._1))<config.get.times){
        true
      }else{
        false
      }
    }).groupByKey().map(x =>{
      var s =""
      var applied_at =""
      var resp = ""
      for(y <- x._2){
        s =s + y._2._1.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]", "")
        applied_at = y._1._1
        resp = y._1._2
      }
      var str = if (s.length>0) new JiebaSegmenter().sentenceProcess(s)
      val fenci = str.toString.replaceAll(","," ")
      val fenciLabel = fenci + "\t__label__" + resp
      x._1 + "," + applied_at + ", "+fenciLabel
      //fenciLabel
    })
    val res_predict2 =v5_predict2.join(smsRow).filter(x =>{
      if((Time.formatDate(x._2._2._2) - Time.formatDate(x._2._1._1))<config.get.times){
        true
      }else{

        false
      }
    }).groupByKey().map(x =>{
      var s =""
      var applied_at =""
      var resp = ""
      for(y <- x._2){
        s =s + y._2._1.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]", "")
        applied_at = y._1._1
        resp = y._1._2
      }
      var str = if (s.length>0) new JiebaSegmenter().sentenceProcess(s)
      val fenci = str.toString.replaceAll(","," ")
      val fenciLabel = fenci + "\t__label__" + resp
      x._1 + "," + applied_at + ", "+fenciLabel
      //fenciLabel
    })

    V5res.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/TextProcess/train")
    res_predict1.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/TextProcess/predict1")
    res_predict2.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/TextProcess/predict2")
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    val schema =
         StructType(
           schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//    val smsDF = spark.createDataFrame(smsRow,schema).cache()
//    import org.apache.spark.sql.functions._
//
//
//
//    val res_train = v5_train.join(smsDF,Seq("uuid"),"left_outer")
//    // 18000 = 60*60*5
//    val res_train_filter = res_train.filter((unix_timestamp(res_train("createtime"))-unix_timestamp(res_train("applied_at"))) <18000)
//    res_train_filter.groupBy("uuid")
//    val tt = (unix_timestamp(res_train("createtime")))
//    res_train.filter(unix_timestamp(res_train("createtime")) >10000).count
//
//
//
//    val res_predict1 = v5_predict1.join(smsDF,Seq("uuid"),"left_outer")
//    val res_predict1_filter = res_predict1.filter((unix_timestamp(res_predict1("createtime"))-unix_timestamp(res_predict1("applied_at"))) <(60*60*5))
//
//    val res_predict2 = v5_predict2.join(smsDF,Seq("uuid"),"left_outer")
//    val res_predict2_filter = res_predict2.filter((unix_timestamp(res_predict2("createtime"))-unix_timestamp(res_predict2("applied_at"))) <(60*60*5))
//
//    val res_predict2 = v5_predict2.join(smsDF,Seq("uuid"),"left_outer")
//    res_predict2.repartition(1).write.option("header", "true").csv("/app/user/data/deeplearning/results/TextProcess/predict2")

//    val app = spark.sql(s"select  userid, smscontent  from deeplearning.user_sms_info where dt ='20170919'").rdd
//
//    val test = spark.read.format("csv").option("header","true").load("/app/user/data/deeplearning/results/TextProcess/testTime.csv").rdd
//    val testRDD = test.map(x =>{
//      val uuid = x.getString(1).trim
//      val time = x.getString(2).trim
//      (uuid,time)
//    })
//    val sq = spark.sql(s"select distinct  userid, createtime from deeplearning.user_sms_info where dt ='20170818' ")
//    val ssss =sq.map(x =>{
//      val uuid = x.getString(0).trim
//      val createtime  = x.getString(1).trim
//      (uuid,createtime)
//    }).rdd
//
//    val accum0 = spark.sparkContext.longAccumulator("in one hour")
//    val accum1 = spark.sparkContext.longAccumulator("in two hour")
//    val accum2 = spark.sparkContext.longAccumulator("in 5 hour")
    import scala.util.control.Breaks._
//    testRDD.join(ssss).groupByKey().foreach(x =>{
//      var flag =true
//      for(y <- x._2 if flag){
//        if((Time.formatDate(y._2) - Time.formatDate(y._1))*1.0 /(1000*60) < 60){
//          accum0.add(1L)
//          flag=false
//        }
//        if((Time.formatDate(y._2) - Time.formatDate(y._1))*1.0 /(1000*60) < 120){
//          accum1.add(1L)
//          flag=false
//        }
//        if((Time.formatDate(y._2) - Time.formatDate(y._1))*1.0 /(1000*60) < 300){
//          accum2.add(1L)
//          flag=false
//        }
//      }
//    })

//    val accum3 = spark.sparkContext.longAccumulator("more ")
//    val accum4 = spark.sparkContext.longAccumulator("less")
//    val accum5 = spark.sparkContext.longAccumulator("more and less")
//    testRDD.join(ssss).groupByKey().foreach(x =>{
//      var flag =true
//      var min = Long.MaxValue
//      var value =Long.MaxValue
//      for(y <- x._2 if flag){
//        if(Time.formatDate(y._2)< min) min = Time.formatDate(y._2)
//        value = Time.formatDate(y._1)
//      }
//      if(value<min) accum3.add(1)
//    })

//    testRDD.join(ssss).foreach(x =>{
//      (Time.formatDate(x._2._1) - Time.formatDate(x._2._2))*1.0 /(1000*60)
//      if(x._2._1<x._2._2){
//
//        accum0.add(1)
//      }
//      accum1.add(1)
//    })



  }


}
