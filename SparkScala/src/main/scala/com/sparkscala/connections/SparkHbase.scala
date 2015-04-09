package com.sparkscala.connections
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object SparkHbase {
case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, args(0))
    val admin = new HBaseAdmin(hbaseConf)
    
    if (!admin.isTableAvailable(args(0))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(0)))
      admin.createTable(tableDesc)
    }
    
    val sc = new SparkContext(conf)


    sc.stop()

  }

}