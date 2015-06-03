package com.sparkscala.MLlib
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vector
import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.commons.cli.{Options, ParseException, PosixParser}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SQLContext

object TwitterClassifierTrainModel {
   val numFeatures = 1000
  val tf = new HashingTF(numFeatures)
     def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }
  def main(args: Array[String]) {
       val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")

    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val people = sqlContext.jsonFile("/conf/tweets_1431440330000/*")
    people.registerTempTable("twittertable")

    val texts = sqlContext.sql("SELECT text from twittertable").map(_.toString)
    
        val vectors = texts.map(featurize).cache()
    vectors.count()  // Calls an action on the RDD to populate the vectors cache.
    val model = KMeans.train(vectors, 4, 20)
    sc.makeRDD(model.clusterCenters, 4).saveAsObjectFile("/conf/")

    val some_tweets = texts.take(100)
    println("----Example tweets from the clusters")
    for (i <- 0 until 20) {
      println(s"\nCLUSTER $i:")
      some_tweets.foreach { t =>        
        if (model.predict(featurize(t)) == i) {
          println(model.predict(featurize(t)))
          println(t)
        }
      }
    }
  }
}