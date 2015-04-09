package com.sparkscala.connections
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext

/**
 * Hello world!
 *
 */
//com.scalaproject.ScalaProject.Application
object Application {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    
    val sc = new SparkContext(conf)
    
    val sqlContext=new org.apache.spark.sql.hive.HiveContext(sc)
    
//    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (name STRING, age INT)")
//    sqlContext.sql("LOAD DATA INPATH 'hdfs://localhost:8020/conf/people.txt' INTO TABLE src")
      sqlContext.sql("SELECT * FROM shopgirl_data_1").collect().foreach(println)
    
    // Queries are expressed in HiveQL 
    
    
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//    import sqlContext.implicits._
//
//    val people = sc.textFile("/conf/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//    people.registerTempTable("people")
//
//    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13")
//    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    sc.stop()
    
    //   conf.set("spark.cassandra.connection.host", "127.0.0.1")
    //     val rdd = sc.cassandraTable("test", "kv") 
    //      println("Valu: "+rdd.count)
    //      println("Valu: "+rdd.first)
    //      println("Valu: "+rdd.map(_.getInt("value")).sum) 

    //     val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    //     collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
    //           val people = sc.textFile("/conf/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    //        people.saveAsCassandraTable("test", "kv2", SomeColumns("name", "age"))
  }

}
