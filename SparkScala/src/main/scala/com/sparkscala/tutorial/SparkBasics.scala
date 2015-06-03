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


object SparkBasics {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile("/conf/conf/people.txt")
    println("Number of Lines: "+textFile.count()+" First Line: "+textFile.first())
    
    val linesWithSpark = textFile.filter(line => line.contains(","))
    
    println("Number of words(, seperated): "+linesWithSpark.count()+" words: "+linesWithSpark.collect().foreach(print))

    val result=linesWithSpark.count()+" words: "+linesWithSpark.fold("")((s1, s2) => s1 + "\t" + s2)

    writeToHdfs("hdfs://localhost:8020","/conf/example_11.txt","Number of words(, seperated): "+result)
    sc.stop()
  }
  
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def writeToHdfs(uri: String, filePath: String, data: String): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val tmpPath = new Path(uri + filePath )
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(tmpPath)))
    writer.write(data)
    writer.close
    fs.close()
  }
}