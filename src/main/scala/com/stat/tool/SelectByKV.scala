package com.stat.tool

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext

object SelectByKV {
  def selectByKV(s: String,  specifiedKV: Array[String]): Boolean = {
    val elems: Array[String] = s.split("\t")
    var lineKV:Map[String,String] = Map()
    for (elem <- elems) {
      var kv: Array[String] = elem.split(":",2)
      if (kv.length == 2) {
        lineKV += (kv(0)->kv(1))
      }
    }
    for (elem <- specifiedKV) {
      var kv: Array[String] = elem.split(":",2)
      if (kv.length == 2) {
        if (!lineKV.contains(kv(0)) || lineKV(kv(0)).equals(kv(1)) == false) {
          return false;
        }
      }
    }
    return true;
  }
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("spark-submit --class com.tool.spark.SelectByKV SparkSample.jar inputFile outputPath K1:V1 K2:V2 K3:V3 ...")
      sys.exit(1)
    }
    val inputFile = args(0)
    val outputPath = args(1)
    val specifiedKV = new Array[String](args.size - 2)
    Array.copy(args, 2, specifiedKV, 0, specifiedKV.size)
    val conf = new SparkConf().setAppName("panyifeng_SelectByKV")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    //对RDD做各种transformation操作
    val result = textFile.filter(line => selectByKV(line.toString(), specifiedKV))
    //对RDD做action操作，只有action才能触发job执行
    result.saveAsTextFile(outputPath)
    sc.stop()
  }
}