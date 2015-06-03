package com.sparkscala.MLlib
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
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
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

object SVMExample {
def main(args: Array[String]) {
    // Load and parse the data file
      val conf = new SparkConf().setMaster("spark://sany:7077").setAppName("SparkSql")
    conf.setJars(List("/home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
  // Load and parse the data file
  val data = sc.textFile("/data/mllib/sample_svm_data.txt")
  val parsedData = data.map { line =>
  val parts = line.split(' ')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(x => x.toDouble).toArray))
  }
  
  // Run training algorithm to build the model
  val numIterations = 20
  val model = SVMWithSGD.train(parsedData, numIterations)
  
  // Evaluate model on training examples and compute training error
 // val labelAndPreds = parsedData.map { point =>
  val point=parsedData.take(1)(0)
  val prediction = model.predict(point.features)
  (point.label, prediction)
  
  val firstDataPoint = parsedData.take(1)(0)
    model.predict(firstDataPoint.features)
 // }
 // val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
 // println("Training Error = " + trainErr)
  }
}