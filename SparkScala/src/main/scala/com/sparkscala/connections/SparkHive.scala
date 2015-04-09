package com.sparkscala.connections
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext

object SparkHive {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)    
    val sqlContext=new org.apache.spark.sql.hive.HiveContext(sc)    
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (name STRING, age INT)")
    sqlContext.sql("LOAD DATA INPATH 'hdfs://localhost:8020/conf/people.txt' INTO TABLE src")
    sqlContext.sql("SELECT * FROM shopgirl_data_1").collect().foreach(println)
    sc.stop()   

  }
}