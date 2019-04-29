package com.pengzhaopeng

import com.mongodb.casbah.Imports.ServerAddress
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 数据加载服务
  */
object DataLoader {

  //源文件 电影 评分 标签
  val MOVIE_DATA_PATH = "movies.csv"
  val RATING_DATA_PATH = "ratings.csv"
  val TAG_DATA_PATH = "tags.csv"

  //collection
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200",
      "mongo.db" -> "recommender"
    )
    val mongoIp1 = "192.168.2.4"
    val mongoIp2 = "192.168.2.5"
    val mongoIp3 = "192.168.2.6"
    val mongoPort = 27200

    //需要创建一个SparkConf配置
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("DataLoader")
      .setMaster(config("spark.cores"))

    //创建一个SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200")
      .getOrCreate()

    import spark.implicits._

    //将Movie数据集加载进来
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //将MovieRDD转换成DataFrame
    val movieDF: DataFrame = movieRDD.map(item => {
      val attr: Array[String] = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
        attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    //将Rating数据集加载进来
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    //将ratingRDD转换成DataFrame
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr: Array[String] = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //将Rating数据集加载进来
    val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)
    //将tagRDD转换成DataFrame
    val tagDF: DataFrame = tagRDD.map(item => {
      val attr: Array[String] = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    //定义连接mongodb的隐士配置
    val addressesList = List(new ServerAddress(mongoIp1, mongoPort), new ServerAddress(mongoIp1, mongoPort), new ServerAddress(mongoIp1, mongoPort))

    implicit val mongoConfig = MongoConfig(addressesList, config("mongo.db"))
    //将数据保存到MongoDB中
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    //将新的数据保存到ES中

    //关闭spark
  }

  /**
    * 将数据保存到MongoDB中
    */
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //新建一个MongoDB的连接 TODO 生产环境配置成连接池 https://www.cnblogs.com/jycboy/p/10077080.html
    val mongoClient = MongoClient(mongoConfig.listAddress)

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF
      .write
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).
      mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION)
    .

    //关闭MongoDB的连接
  }
}


/**
  * MongoDB的连接配置
  *
  * @param listAddress 集群地址
  * @param db          数据库
  */
case class MongoConfig(listAddress: List[ServerAddress], db: String)

/**
  * Movie数据集，数据集字段通过分割
  *
  * 151^                          电影的ID
  * Rob Roy (1995)^               电影的名称
  * In the highlands ....^        电影的描述
  * 139 minutes^                  电影的时长
  * August 26, 1997^              电影的发行日期
  * 1995^                         电影的拍摄日期
  * English ^                     电影的语言
  * Action|Drama|Romance|War ^    电影的类型
  * Liam Neeson|Jessica Lange...  电影的演员
  * Michael Caton-Jones           电影的导演
  *
  * tag1|tag2|tag3|....           电影的Tag
  **/
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * Tag数据集，用户对于电影的标签数据集，用，分割
  *
  * 15,          用户的ID
  * 1955,        电影的ID
  * dentist,     标签的具体内容
  * 1193435061   用户对于电影打标签的时间
  */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)