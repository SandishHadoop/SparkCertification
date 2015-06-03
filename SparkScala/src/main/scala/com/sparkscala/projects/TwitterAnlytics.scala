package com.sparkscala.projects
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vector
import com.google.gson.{ GsonBuilder, JsonParser }
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.clustering.KMeans
import org.apache.commons.cli.{ Options, ParseException, PosixParser }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Time, Seconds, StreamingContext }
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
//com.sparkscala.projects.TwitterAnlytics
object TwitterAnlytics {
  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  def main(args: Array[String]): Unit = {
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, List("hadoop", "hi"))

    val streamingSQl = stream.map(status => (status.getText + "\t" + status.getCreatedAt)).window(Seconds(30))

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val hashTagsbyWindow = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    println("---------------------:Println:----------")

    streamingSQl.foreachRDD(rdd => {
      println("---------------------:Println:----------4")
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      println("---------------------:Println:----------5")
      val wordsDataFrame = rdd.map(w => w.split("\t")).map(p => Twitter(p(0), p(1))).toDF()
      println("---------------------:Println:----------6")
      wordsDataFrame.registerTempTable("TwitterAnalytics")

      val teenagers = sqlContext.sql("SELECT text FROM TwitterAnalytics").map(_.toString)

      val model = new KMeansModel(sc.objectFile[Vector]("/conf/TWitterKmeanModel/part*").collect())

      // val jsonfile=sqlContext.jsonFile("/conf/tweets_1431440330000/part*")
      //  jsonfile.registerTempTable("Twitter")
      val texts = sqlContext.sql("SELECT text FROM TwitterAnalytics").map(_.toString)

      println("----Example tweets from the clusters")
      for (i <- 0 until 20) {
        println(s"\nCLUSTER $i:")
        texts.foreach { t =>
          if (model.predict(featurize(t)) == i) {
            println(t)
          }
        }
      }

    })

    hashTagsbyWindow.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class Twitter(text: String, CreatedAt: String)
  def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }
  object SQLContextSingleton {
    @transient private var instance: SQLContext = null

    // Instantiate SQLContext on demand
    def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
}
