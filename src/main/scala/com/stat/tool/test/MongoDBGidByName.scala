package com.stat.tool.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext
import com.mongodb.casbah.Imports._
//import com.mongodb.casbah.query.Imports._
//import com.mongodb.casbah.Imports.{ $and, $or }
import scala.util.parsing.json.JSON
import com.mongodb.BasicDBList
import java.io.PrintWriter
import java.io.File

object MongoDBGidByName {
  def main(args: Array[String]) {
    println("Automatically detect SCRAM-SHA-1 or Challenge Response protocol")
    // Automatically detect SCRAM-SHA-1 or Challenge Response protocol
    val server = new ServerAddress("10.172.195.210", 7476)
    val credentials = MongoCredential.createCredential("mongo", "admin", "708a835a643487dd".toCharArray())
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient("btime_news_indexdata")
    val collection = db("news_info")
    //val query = com.mongodb.casbah.query.Imports.MongoDBObject($and($or(("title" -> ".*习近平.*".r),("summary" -> ".*习近平.*".r),("content" -> ".*习近平.*".r),("title" -> ".*李克强.*".r),("summary" -> ".*李克强.*".r),("content" -> ".*李克强.*".r),("title" -> ".*张德江.*".r),("summary" -> ".*张德江.*".r),("content" -> ".*张德江.*".r),("title" -> ".*俞正声.*".r),("summary" -> ".*俞正声.*".r),("content" -> ".*俞正声.*".r),("title" -> ".*刘云山.*".r),("summary" -> ".*刘云山.*".r),("content" -> ".*刘云山.*".r),("title" -> ".*王岐山.*".r),("summary" -> ".*王岐山.*".r),("content" -> ".*王岐山.*".r),("title" -> ".*张高丽.*".r),("summary" -> ".*张高丽.*".r),("content" -> ".*张高丽.*".r),("title" -> ".*彭丽媛.*".r),("summary" -> ".*彭丽媛.*".r),("content" -> ".*彭丽媛.*".r),("title" -> ".*胡锦涛.*".r),("summary" -> ".*胡锦涛.*".r),("content" -> ".*胡锦涛.*".r),("title" -> ".*温家宝.*".r),("summary" -> ".*温家宝.*".r),("content" -> ".*温家宝.*".r),("title" -> ".*江泽民.*".r),("summary" -> ".*江泽民.*".r),("content" -> ".*江泽民.*".r),("title" -> ".*国母.*".r),("summary" -> ".*国母.*".r),("content" -> ".*国母.*".r),("title" -> ".*彭妈妈.*".r),("summary" -> ".*彭妈妈.*".r),("content" -> ".*彭妈妈.*".r),("title" -> ".*彭麻麻.*".r),("summary" -> ".*彭麻麻.*".r),("content" -> ".*彭麻麻.*".r),("title" -> ".*习大大.*".r),("summary" -> ".*习大大.*".r),("content" -> ".*习大大.*".r),("title" -> ".*习主席.*".r),("summary" -> ".*习主席.*".r),("content" -> ".*习主席.*".r),("title" -> ".*习总.*".r),("summary" -> ".*习总.*".r),("content" -> ".*习总.*".r),("title" -> ".*习书记.*".r),("summary" -> ".*习书记.*".r),("content" -> ".*习书记.*".r)),"source" -> "open_cms"))
    val nameList = "习近平,李克强,张德江,俞正声,刘云山,王岐山,张高丽,彭丽媛,胡锦涛,温家宝,江泽民,国母,彭妈妈,彭麻麻,习大大,习主席,习总,习书记".split(",")
    val fields = MongoDBObject("gid" -> 1, "source" -> 1, "_id" -> 0)

    for (name <- nameList) {
      var writer = new PrintWriter(new File("E:/gid/title_" + name + ".txt"))
      var query = MongoDBObject("title" -> (".*" + name + ".*").r) ++ ("source" $in ("opencms", "opencms_ops"))
      var result = collection.find(query, fields)
      result.foreach { x =>
        writer.write("%s\t%s\n".format(x.get("gid"), x.get("source")))
      }
      writer.close()

      writer = new PrintWriter(new File("E:/gid/summary_" + name + ".txt"))
      query = MongoDBObject("summary" -> (".*" + name + ".*").r) ++ ("source" $in ("opencms", "opencms_ops"))
      result = collection.find(query, fields)
      result.foreach { x =>
        writer.write("%s\t%s\n".format(x.get("gid"), x.get("source")))
      }
      writer.close()

      writer = new PrintWriter(new File("E:/gid/content_" + name + ".txt"))
      query = MongoDBObject("content" -> (".*" + name + ".*").r) ++ ("source" $in ("opencms", "opencms_ops"))
      result = collection.find(query, fields)
      result.foreach { x =>
        writer.write("%s\t%s\n".format(x.get("gid"), x.get("source")))
      }
      writer.close()
    }
  }
}