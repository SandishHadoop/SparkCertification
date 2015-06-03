package com.sparkscala.projects

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._
object ApacheLogAnalysis {

  case class ApacheAccessLog(ipAddress: String, clientIdentd: String,
    userId: String, dateTime: String, method: String,
    endpoint: String, protocol: String,
    responseCode: Int, contentSize: Long) {

  }

  object ApacheAccessLog {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

    def parseLogLine(log: String): ApacheAccessLog = {
      val res = PATTERN.findFirstMatchIn(log)
      if (res.isEmpty) {
        throw new RuntimeException("Cannot parse log line: " + log)
      }
      val m = res.get
      ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
        m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
    }
  }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala")
    val sc = new SparkContext(sparkConf)
    val logs = sc.textFile("").map(ApacheAccessLog.parseLogLine).cache()
    val contentSizes = logs.map(log => log.contentSize).cache()
    println("contentSizes MAX:"+contentSizes.max +" contentSizes MIN:"+contentSizes.min+" contentSizes"+contentSizes.reduce(_+_)/contentSizes.count)
    
    val iplist = logs.map(log => log.ipAddress).cache()
    println("MAXIMUN OCCURANCE OF SINGLE IP:"+iplist.map(ip=>(ip,1)).reduceByKey(_+_).filter(f=>f._2>10).map((_._1)).take(100).mkString("[", ",", "]"))
       
    val endpoint = logs.map(log => log.endpoint).cache()
    println("MAXIMUN OCCURANCE OF SINGLE IP:"+endpoint.map(ip=>(ip,1)).reduceByKey(_+_).sortBy(f=>f._2,false).take(10).mkString("[", ",", "]"))
        
    
  }
}