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

object SelfContainedApplication {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/conf/README.md",2).cache() 
    val linesAs=textFile.filter { line => line.contains("a") }
    val linesBs=textFile.filter { line => line.contains("b") }
    linesAs.collect().foreach(println)
    linesBs.collect().foreach(println)
    println("count of A's: "+linesAs.count()+" count of B's"+linesBs.count())    
    sc.stop()
  }
  def printFucntion(arg:String,arg2:String){
    println("line"+arg)    
  }
  
}