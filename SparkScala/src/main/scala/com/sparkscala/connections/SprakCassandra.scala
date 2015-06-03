package com.sparkscala.connections
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
//com.sparkscala.connections.SprakCassandra
object SprakCassandra {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val rdd = sc.cassandraTable("test", "kv") 
    println("Valu: "+rdd.count)
    println("Valu: "+rdd.first)
    //println("Valu: "+rdd.map(_.getInt("value")).sum) 
    
    val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
    val people = sc.textFile("/conf/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.saveAsCassandraTable("test", "kv2", SomeColumns("name", "age"))
     
    sc.stop()
  }
}