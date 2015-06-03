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
import org.apache.spark.rdd.RDD

object KeyValuePairs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/conf/README.md") 
    val pairs = lines.map(s => (s, 1)) 
    val counts = pairs.reduceByKey((a, b) => a + b) 
    counts.sortByKey().collect()
  }
}