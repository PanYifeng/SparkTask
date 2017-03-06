package com.stat.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext

object GidNull {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("spark-submit --class com.tool.spark.GidNull SparkSample.jar inputFile outputPath")
      sys.exit(1)
    }
    val inputFile = args(0)
    val outputPath = args(1)
    val conf = new SparkConf().setAppName("panyifeng_GidNull")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    //对RDD做各种transformation操作
    val actionSet = Set("view", "click")
    val result = textFile.filter(line => AppLiveData.getVal(line.toString(), "gid").equals("")).filter(line => actionSet.contains(AppLiveData.getVal(line.toString(), "action")))
    //对RDD做action操作，只有action才能触发job执行
    result.saveAsTextFile(outputPath)
    sc.stop()
  }
}