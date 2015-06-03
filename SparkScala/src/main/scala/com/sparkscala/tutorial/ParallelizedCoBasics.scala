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

object ParallelizedCoBasics {
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5) 
    val distData = sc.parallelize(data)
    
    val lines = sc.textFile("/conf/README.md") 
    val lineLengths = lines.map(s => s.length) 
    lineLengths.persist() 
    val totalLength = lineLengths.reduce((a, b) => a + b)
    
    sc.stop()
  }

}