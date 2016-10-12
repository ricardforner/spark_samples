package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WorldCount {
  
  def main(args: Array[String]) = {
    
    val conf = new SparkConf()
      .setAppName("WorldCount")
      .setMaster("local")
//      .setMaster("spark://192.168.1.100:7077")
     val sc = new SparkContext(conf)
    
    val test = sc.textFile("food.txt")
    test.flatMap { line =>
      line.split(" ")
    }
    .map { word =>
      (word,1)
    }
    .reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")
    
  }
  
}