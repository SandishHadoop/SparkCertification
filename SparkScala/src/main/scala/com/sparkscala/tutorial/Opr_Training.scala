package com.sparkscala.tutorial
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import SparkContext._

object Opr_Training {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("Opr_Training")
    val sc = new SparkContext(conf)
    //load data file from hdfs 
    val file = sc.textFile("hdfs://localhost:8020/mnt/omega/pagecounts/")
    //slip the file content by “\n”
    val entries = file.flatMap(line => line.split("\n"))
    //get the count of no of lines present in the file 
    entries.count
    //split lines by space count language(2 column) 
    val count = entries.map(line => (line.split(" ")(1), 1)).reduceByKey((w, c) => w + c)
    count.foreach(println)
    //sortByKey and take first 100 items 
    val orderByKeyCount = count.sortByKey(true).take(100).foreach(println)
    //sort key by using sortBy 
    val sortbyKeyCount = count.sortBy(c => c._1, false).take(100).foreach(println)
    //sort value by using sortBy 
    val sortbyValueCount = count.sortBy(c => c._2, true).take(100).foreach(println)
  }
}