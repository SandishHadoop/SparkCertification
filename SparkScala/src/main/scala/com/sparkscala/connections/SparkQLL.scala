package com.sparkscala.connections
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
object SparkQLL {
 case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val people = sc.textFile("/conf/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    sc.stop()

  }

}