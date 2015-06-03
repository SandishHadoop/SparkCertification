package com.sparkscala.projects

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import java.util.{Date, Locale}
import java.text.DateFormat
import java.text.DateFormat._

object TwitterAnalyticsFromJsonFile {
  def main(args: Array[String]): Unit = {
    case class Twitter(id:String,tweet:String,created_at:String,Fcount:String,Rcount:String,Glocation:String,country:String,username:String)
    val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala")
    val sc = new SparkContext(sparkConf)
    val people = sc.textFile("/mnt/data/Twitter_Data_1.csv")
    val formatter =  new java.text.SimpleDateFormat("dd MM yyyy ");
    val Twitters=people.map(s=>s.split("\t")).map(t=>Twitter(t(0).toString(),t(1).toString(),t(2).toString(),t(3).toString(),t(4).toString(),t(5).toString(),t(6).toString(),t(7).toString()))
    val users=Twitters.map(p=>p.id)
    //find top  users
    println(""+users.map(p=>(p,1)).reduceByKey(_+_).sortBy(_._2,false).take(100).mkString("[", ",", "]"))
    
    val countrys=Twitters.map(p=>p.country)
    //find top  users
    val sav=countrys.map(p=>(p,1)).reduceByKey(_+_).sortBy(_._2,false)
    //
    
    
    println(""+countrys.map(p=>(p,1)).reduceByKey(_+_).sortBy(_._2,false).take(100).mkString("[", ",", "]"))
    
    
    val created_at=Twitters.map(p=>p.created_at)
    //find top  users
    println(""+created_at.map(p=>(p,1)).reduceByKey(_+_).sortBy(_._2,false).take(100).mkString("[", ",", "]"))
    
  }
}