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

object PassingFunctionstoSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
    val sc = new SparkContext(conf)
    val data = Array("1", "2", "3", "4", "5") 
    val distData = sc.parallelize(data)
    val array=distData.collect()
    val rrd=doStuff(distData)
    rrd.collect().foreach { r => println(r) }
  }
  
  class MyClass { 
    def func1(s: String): String = {"1"} 
    def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(func1) }
  }

  def doStuff(rdd: RDD[String]): RDD[String] = { 
    val field_ = "1" 
    rdd.map(x => field_ + x)
  }
}