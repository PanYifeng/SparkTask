package com.stat.tool.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext
import com.mongodb.casbah.Imports._
import scala.util.parsing.json.JSON
import com.mongodb.BasicDBList

object MongoDBTest {
  def main(args: Array[String]) {
    println("Automatically detect SCRAM-SHA-1 or Challenge Response protocol")
    // Automatically detect SCRAM-SHA-1 or Challenge Response protocol
    val server = new ServerAddress("10.172.195.210", 7476)
    val credentials = MongoCredential.createCredential("mongo", "admin", "708a835a643487dd".toCharArray())
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient("btime_news_indexdata")
    val collection = db("news_info")
    val query = MongoDBObject("gid" -> "23hsocs6fckvur05ik3i49u0me2", "news_source" -> "天津高清")
    val fields = MongoDBObject("gid" -> 1, "news_source" -> 1, "update_time" -> 1, "streams" -> 1, "_id" -> 0)
    val result = collection.findOne(query, fields)
    result.foreach { x =>
      println("Found ! %s".format(x))
      println("Found a update_time! %s".format(x.get("update_time")))
      println("Found a streams! %s".format(x.get("streams")))
      //println("Found a stream_url! %s".format(streams.get("stream_url")))
      val streams = JSON.parseFull("%s".format(x.get("streams")))
      streams match {
        // Matches if jsonStr is valid JSON and represents a Map of Strings to Any  
        case Some(map: Map[String, String]) => println("map: " + map)
        case Some(list: List[Map[String, String]]) =>
          println("list: " + list)
          list.foreach { stream => println("Found a stream_url! %s".format(stream.get("stream_url"))) }
        case None  => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }

    }
  }
}