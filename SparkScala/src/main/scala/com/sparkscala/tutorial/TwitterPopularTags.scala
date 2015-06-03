package com.sparkscala.tutorial
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf


object TwitterPopularTags {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
   // StreamingExamples.setStreamingLogLevels()
   //pVlz3waSTx1r8l0nYlP6TA S27ZQwyxCK8YvBCqH3vo4L8udxCacEpI50gOA04Xm0 354892921-DwCuCPdFHUVgsc76gOexVsR1wuPYK3mKXjUq9ERQ u5PpySMJgJmDnErt3NEyxYcsoqBInXtNJLAKkcHLg
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setMaster("spark://sany:7077").setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    
     val fullObjs = stream.map { x => x.getId+"\t"+x.getCreatedAt+"\t"+x.getPlace+"\t"+x.getRetweetCount+"\t"+x.getText+"\t"+x.getSource }.foreach{count => count.saveAsTextFile("/conf/Twitterdata.txt")}
     
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
                     

 val result =""

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    //  result.concat(""+topList.fold("")((s1, s2) => s1 + "\t" + s2))
       rdd.saveAsTextFile("/conf/rddtop.txt")
      
   //  println("=====================================\n"+topList.map(d => s"$result=$d").mkString("[", ",", "]"))
    })
    
    
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
 
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
    
   
    ssc.start()
    
    ssc.awaitTermination()
  }
}