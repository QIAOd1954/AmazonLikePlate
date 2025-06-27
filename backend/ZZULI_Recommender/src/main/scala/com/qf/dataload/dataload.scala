package com.qf.dataload

//import com.mongodb.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * product 数据集
 * 3982   商品ID
 * 外设产品   商品分类
 * 商品UGC标签
 *
 */
case class Product(productId:Int,name:String,categories: String,imageUrl:String,tags:String)
/**
 * product 数据集(评分)
 * 4867     用户ID
 * 5.0      评分
 * 1395676800  时间戳
 * 457976    商品ID
 */
case class Rating(userId:Int,productId:Int,score:Double,timestamp:Long)
/**
 * Mongodb连接配置
 * @param uri Mongodb的连接URI
 * @param db 要操作的db
 */
case class MongoConfig(uri:String,db:String)
object dataload {
      // 定义数据文件的路径
      val PRODUCT_DATA_PATH="D:\\JAVA\\idea\\backendcode\\backend\\ZZULI_Recommender\\src\\main\\resources\\products.csv"
      val RATING_DATA_PATH="D:\\JAVA\\idea\\backendcode\\backend\\ZZULI_Recommender\\src\\main\\resources\\ratings.csv"
      // 定义MONGODB中存储的表名
      val MONFODB_PRODUCT_COLLECTION="Product"
      val MONGODB_RATING_COLLECTION="Rating"

//      主程序主函数
def main(args: Array[String]): Unit = {
  val config=Map(
    "spark.cores"-> "local[*]",
    "mongo.uri" -> "mongodb://192.168.111.128:27017/recommender",
    "mongo.db" -> "recommender"
  )
    //  创建一个SParkConf的配置
    val sparkConf=new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
  //  创建一个SparkSession的配置
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
//  在对DataFrame以及DATASET进行操作都需要这个包的支持
  import spark.implicits._
  //创建product的RDD
  val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
  //将RDD转换成DATAFRAME
  val productDF= productRDD.map(item => {
    val attr=item.split("\\^")
   // 作用：trim 去除字符串首尾的空格
    Product(attr(0).toInt,attr(1).trim,attr(2).trim,attr(4).trim,attr(5).trim)
  }).toDF()
  //创建Rating的RDD
  val ratingRDD=spark.sparkContext.textFile(RATING_DATA_PATH)
  //将RDD转换成DATAFRAME
  val ratingDF=ratingRDD.map(item=>{
    val attr=item.split(",")
    Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toLong)
  }).toDF()

  //打印Dataframe
  productDF.show()
  ratingDF.show()

  //声明一个隐式的配置对象
  implicit val mongoConfig=MongoConfig(config.get("mongo.uri").get,config("mongo.db"))


  //保留数据到Mongodb数据库中
  storeDataInMongoDB(productDF,ratingDF)
  //  关闭Spark
  spark.stop()
}
  def storeDataInMongoDB(productDF:DataFrame,ratingDF:DataFrame)(implicit mongoConfig: MongoConfig) = {
    //创建mongodb的客户端的连接
    val mongoClient=MongoClient(MongoClientURI(mongoConfig.uri))

    //定义通过MongoDB客户端拿到的表操作对象
    val productCollection=mongoClient(mongoConfig.db)(MONFODB_PRODUCT_COLLECTION)
    val ratingCollection=mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
 //如果MOngoDB中有对应的数据库则应该删除
     productCollection.dropCollection()
     ratingCollection.dropCollection()
 //将当前数据写入到Mongodb当中
    productDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONFODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建立一个索引
    productCollection.createIndex(MongoDBObject("productID"->1))
    ratingCollection.createIndex(MongoDBObject("userID"->1))
    ratingCollection.createIndex(MongoDBObject("productID"->1))
    //关闭Mongodb的连接
    mongoClient.close()
  }

}
