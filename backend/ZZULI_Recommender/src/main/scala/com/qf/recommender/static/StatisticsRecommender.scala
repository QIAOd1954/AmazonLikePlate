package com.qf.recommender.static

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//统计评分次数
case class Rating(userId:Int,productId:Int,score:Double,timestamp: Long)

case class MongoConfig(uri:String,db:String)

object StatisticsRecommender {
  //原始数据集,常量要大写
  val MONGODB_RATING_COLLECTION="Rating"
  //统计表的名称
  //历史上打分次数最多的
  val RATE_MORE_PRODUCTS="RateMoreProducts"
  //以月为单位打分次数最多的
  val RATE_MORE_RECENTLY_PRODUCTS="RateMoreRecentlyProducts"
  //平均分
  val AVERAGE_PRODUCTS="AverageProducts"
//main方法
  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.cores"->"local[*]",
      "mongo.uri"->"mongodb://192.168.111.128:27017/recommender",
      "mongo.db"->"recommender"
      )
    //创建SparkConf配置
    val sparkConf=new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))
    //加入隐式转换
    import spark.implicits._
    val ratingDF=spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张名称为ratings的表
    ratingDF.createOrReplaceTempView("ratings")
    //1.统计所有历史数据当中每个商品的打分次数----历史热门推荐
    //数据结构-> productId,count
    val rateMoreProductsDF=spark.sql(
    """
       | select
       | productId,
       | count(productId) as count
       | from ratings
       | group by productId
       |""".stripMargin)

    println("------------------------------------")
    rateMoreProductsDF.show(10)

//    def tranTimeToString(tm:Long):String={
//      val fm =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val time=fm.format(new Date(tm))
//      tim
//    }
    //调用存储MongoDB函数
    storeDFInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)
    //2.统计以月为单位每个商品的评分次数
    //数据结构：productID，count，time
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个日期的函数
    spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
    //将原来的Rating数据集中的时间转换成年月的格式
    val ratingOfYearMonthDF=spark.sql(
      """
         |select
         |productId,
         |score,
         |changeDate(timestamp) as yearmonth
         |from
         |ratings
         """.stripMargin)

    println("-----------------------------")
    ratingOfYearMonthDF.show(10)
    //将新的数据集注册成一张表
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    //统计最近一个月商品的打分次数
    val rateMoreRecentlyProductsDF=spark.sql(
      """
        |select
        |productId,
        |count(productId) as count,
        |yearmonth
        |from
        |ratingOfMonth
        |group by
        |yearmonth,productId
        |order by
        |yearmonth desc,count desc
        |""".stripMargin )
    println("-------------------------")
    rateMoreRecentlyProductsDF.show(10)
    storeDFInMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)


    //3.统计每个商品的平均评分-----大家都喜欢或者大家都在看的推荐

   val averageProductsDF=spark.sql(
      """
        |select
        |productId,
        |avg(score) as avg
        |from
        |ratings
        |group by
        |productId
        |""".stripMargin)
    println("--------------------------")
    averageProductsDF.show(10)
    storeDFInMongoDB(averageProductsDF,AVERAGE_PRODUCTS)
    spark.stop()
  }
  /**
   * 存储到MongoDB
   * @param DF
   * @param collection_name
   * @param mongoConfig
   */
  def storeDFInMongoDB(DF:DataFrame,collection_name:String)(implicit mongoConfig: MongoConfig)={
    DF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
