package com.sparkscala.MLlib
import java.io.File

import com.google.gson.Gson
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._

import twitter4j.TwitterStream
object TwitterClassifierCollecter {
  private var gson = new Gson()
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var partitionsEachInterval = 50
  private var numTweetsToCollect = 1000

  def main(args: Array[String]) {
    System.setProperty("twitter4j.oauth.consumerKey", "PUdodpFh9GKr0NWDAqdm3g")
    System.setProperty("twitter4j.oauth.consumerSecret", "0sS6vCFrjhwI5Mip9kmmf1Fb8Bbu72K5zo4kagZKnC8")
    System.setProperty("twitter4j.oauth.accessToken", "354892921-n3v2TDh4EZKE6eF7PzaW5LadbVpRgfysi66J9cqk")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "9SfnhzwuKFRemh34M339OjXdsurom3yxKzauvHoDA09DY")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
     conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT-jar-with-dependencies.jar"))
    val ssc = new StreamingContext(conf, Seconds(5))
    val streamJson = TwitterUtils.createStream(ssc, None, Array("Hadoop", "Data", "Stats","hi")).map (gson.toJson(_))
    
    streamJson.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        outputRDD.collect().foreach(println)
        outputRDD.saveAsTextFile("/conf/" + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })
        ssc.start()
    ssc.awaitTermination()
  }
}