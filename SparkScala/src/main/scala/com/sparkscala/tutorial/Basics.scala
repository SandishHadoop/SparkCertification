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

object Basics {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/conf/README.md")
    println(textFile.name + " file contains " + textFile.count() + " lines")
    println(textFile.name + " has fist line has: " + textFile.first())
    val linesWithSpark = textFile.filter(line => line.contains("Spark"))
    println(textFile.name + " has spark words count: " + textFile.filter(line => line.contains("Spark")).count())
    sc.stop()
  }
}