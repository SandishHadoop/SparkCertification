//package com.sparkscala.tutorial
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import java.io.BufferedWriter
//import java.io.FileWriter
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.conf.Configuration
//import java.io.OutputStreamWriter
//import org.apache.hadoop.fs.Path
//import java.util.Random
//import org.apache.spark.rdd.RDD
//import org.apache.spark.AccumulatorParam
//
//
//object SharedVariables {
//   def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkBasics")
//    val sc = new SparkContext(conf)
//         
//    val broadcastVar = sc.broadcast(Array(1, 2, 3)) 
//    broadcastVar.value
//    
//    val accum = sc.accumulator(0, "My Accumulator") 
//    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x) 
//    accum.value 
//    object VectorAccumulatorParam extends AccumulatorParam[Vector]{ 
//    def zero(initialValue: Vector): Vector = { 
//    Vector.zeros(initialValue.size) 
//    } 
//    def addInPlace(v1: Vector, v2: Vector): Vector = { 
//    v1 += v2 
//    } 
//    } 
//   
//    val acc = sc.accumulator(0) 
//    data.map(x => acc += x; f(x))
//  }
//}