package com.stat.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext

import com.stat.tool.SelectByKV

import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.json4s._
import org.json4s.native.JsonMethods._

object AppViewClick {
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("spark-submit --class com.tool.spark.SelectByKV SparkSample.jar inputFile outputPath date")
      sys.exit(1)
    }
    val inputFile = args(0)
    val outputPath = args(1)
    val dateInfo = args(2)
    val conf = new SparkConf().setAppName("panyifeng_AppLiveData")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val fs = FileSystem.get(new Configuration())
    val writer = new PrintWriter(fs.create(new Path(outputPath)))
   
    val viewKV = Array("action:view")
    val view = textFile.filter(line => SelectByKV.selectByKV(line, viewKV)).count
    
    val clickKV = Array("action:click")
    
    writer.close
    sc.stop()
  }
}