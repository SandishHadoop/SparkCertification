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
import SparkContext._

object RDDOperations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
    val sc = new SparkContext(conf)
    
    val textFile=sc.textFile("/conf/README.md")
    
    val lines=textFile.flatMap { lines => lines.split(" ") }.map { word => (word,1) }.reduceByKey(_+_)
    lines.collect().foreach(printFucntion)
    println("WordCount"+lines.count()+" words count"+lines.foreach(println))
    
    val maxline=textFile.map { line => line.split(" ").size }.reduce ( ( a , b ) => if ( a > b ) a else b)
    println("maxline "+maxline)
    sc.stop()
  }
  def printFucntion(arg:(String,Int)){
    println("line"+arg._1)
    println("line"+arg._2)
  }
}