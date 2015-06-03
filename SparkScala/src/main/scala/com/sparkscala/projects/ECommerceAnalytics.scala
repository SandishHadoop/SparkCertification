package com.sparkscala.projects
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import java.util.{ Date, Locale }
import java.text.DateFormat
import java.text.DateFormat._

object ECommerceAnalytics {
  def checkNull(value: String){
    //val tt=value
    if (!value.isEmpty()) {
      value
    }
    
  }
  def main(args: Array[String]): Unit = {
    case class Ecommerce(department: String, brand: String, category: String, subCategory: String, collection: String, colour: String, colourImageUrl: String, description: String, manufacturer: String, partNo: String, material: String, modelNo: String, msrp: String, currency: String, offers: String, dimensionLength: String, dimensionWidth: String, dimensionHeight: String, dimensionWeight: String, title: String, madeIn: String, salePrice: String)
    val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala")
    val sc = new SparkContext(sparkConf)
    val eCommerce = sc.textFile("/mnt/data/ecommerce_1.csv")

    val eCommerceObjs = eCommerce.map(ob => ob.split("\t")).map(ob => Ecommerce(ob(4).toString(), ob(5).toString(), ob(7).toString(), ob(8).toString(), ob(9).toString(), ob(10).toString(), ob(11).toString(), ob(12).toString(), ob(13).toString(), ob(14).toString(), ob(15).toString(), ob(16).toString(), ob(17).toString(), ob(18).toString(), ob(19).toString(), ob(20).toString(), ob(21).toString(), ob(22).toString(), ob(23).toString(), ob(24).toString(), ob(25).toString(), ob(26).toString()))

    // find Which has slrp more then 15
    println("" + eCommerceObjs.filter(p => p.salePrice.toInt > 15).take(70).mkString("[", ",", "]"))

    println("" + eCommerceObjs.map(p => ((p.msrp.toDouble - p.salePrice.toDouble) / 100) * p.salePrice.toDouble).take(70).mkString("[", ",", "]"))

    println(""+eCommerceObjs.filter(p=>(!p.category.isEmpty())).take(70).mkString("[",",","]"))

  }

}