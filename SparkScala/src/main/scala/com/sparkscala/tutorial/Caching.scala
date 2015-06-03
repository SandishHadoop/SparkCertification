package com.sparkscala.tutorial
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.OutputStreamWriter
import org.apache.hadoop.fs.Path
import java.util.Random

object Caching {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/conf/README.md")
    val linesWithSpark = textFile.filter(line => line.contains("Spark"))
    linesWithSpark.cache()
    println("count after cache 1st: "+linesWithSpark.count())
    println("count after cache 2nd: "+linesWithSpark.count())
    sc.stop()
  }

}