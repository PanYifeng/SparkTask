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

object AppLiveData {
  //不加这一句会提示formats找不到，并且还提示了将org.json4s.DefaultFormats提到前面
  implicit val formats = DefaultFormats

  def myFilter(s: String, executeDate: String): Boolean = {
    val elems: Array[String] = s.split("\t")
    if (elems.length < 3) {
      return false
    }
    if (!(elems(2).toLowerCase().equals("btime.api.click"))) {
      return false
    }
    if (elems(0).length < 10) {
      println("date length < 10:" + elems(0))
      return false
    }
    val serverDate = elems(0).substring(0, 10)
    if (!serverDate.equals(executeDate)) {
      return false
    }
    for (elem <- elems) {
      var kv: Array[String] = elem.split(":", 2)
      if (kv.length == 2) {
        if (kv(0).equals("client_id")) {
          if (kv(1) == null || (!"3".equals(kv(1)) && !"4".equals(kv(1)))) {
            return false
          }
        }
      }
    }
    return true
  }
  def getVal(line: String, key: String): String = {
    val elems: Array[String] = line.split("\t")
    for (elem <- elems) {
      var kv: Array[String] = elem.split(":", 2)
      if (kv.length == 2) {
        if (kv(0).equals(key)) {
          return kv(1)
        }
      }
    }
    return ""
  }
  def getCid(line: String): String = {
    val position = getVal(line, "channel_position")
    if (position == null || "".equals(position)) {
      return ""
    }
    try {
      val posList: List[String] = parse(position, false).extract[List[String]]
      if (posList.size == 3) {
        // "channel_position":["页面id","channel_id/item","板块名称"]
        return posList(1)
      }
    } catch {
      case ex: ParserUtil$ParseException => {
        println("ParserUtil$ParseException")
        return ""
      }
    }
    return ""
  }
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
    val textFile = sc.textFile(inputFile).filter(line => myFilter(line, dateInfo))
    val fs = FileSystem.get(new Configuration())
    val writer = new PrintWriter(fs.create(new Path(outputPath)))

    //    //App底部按钮点击次数
    //    val appTabKV = Array("action:app_tab_click")
    //    val appTabFilter = textFile.filter(line => SelectByKV.selectByKV(line, appTabKV))
    //    val appTabRes = appTabFilter.map(line => (getVal(line, "pid"), 1)).reduceByKey(_ + _)
    //    appTabRes.collect().foreach {
    //      res =>
    //        {
    //          val myPid = res._1
    //          val myCount = res._2
    //          writer.println(dateInfo + "\tapp_tab_click\t" + myPid + "\t" + myCount)
    //        }
    //    }
    //    //直播间PV/UV
    //    val liveRoomKV = Array("action:live_room")
    //    val liveRoomFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveRoomKV))
    //    val liveRoomRes = liveRoomFilter.map(line => getVal(line, "mid"))
    //    val liveRoomPv = liveRoomRes.count()
    //    val liveRoomUv = liveRoomRes.distinct().count()
    //    //    val liveRoomRes = liveRoomFilter.map(line => (getVal(line, "mid"), 1)).reduceByKey(_ + _)
    //    //    var pv = 0
    //    //    var uv = 0
    //    //    liveRoomRes.collect().foreach {
    //    //      res =>
    //    //        {
    //    //          val myCount = res._2
    //    //          pv += myCount
    //    //          uv += 1
    //    //        }
    //    //    }
    //    writer.println(dateInfo + "\tlive_room\t" + liveRoomPv + "\t" + liveRoomUv)
    //    //直播间停留时长
    //    val liveDurationKV = Array("action:live_duration")
    //    val liveDurationFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveDurationKV))
    //    val liveDurationRes = liveDurationFilter.map(line => (getVal(line, "init_module"), 1)).reduceByKey(_ + _)
    //    var durCountMap: Map[String, Long] = Map()
    //    liveDurationRes.collect().foreach {
    //      res =>
    //        {
    //          val myType = res._1
    //          val myCount = res._2
    //          durCountMap += (myType -> myCount.toLong)
    //          //writer.println(dateInfo + "\tlive_duration\t" + myType + "\t" + myCount)
    //        }
    //    }
    //    val liveDurationSum = liveDurationFilter.map(line => (getVal(line, "init_module"), getVal(line, "duration").toLong)).reduceByKey(_ + _)
    //    liveDurationSum.collect().foreach {
    //      res =>
    //        {
    //          val myType = res._1
    //          val myDur = res._2
    //          val myCount: Long = durCountMap(myType)
    //          writer.println(dateInfo + "\tlive_duration\t" + myType + "\t" + myCount + "\t" + myDur)
    //        }
    //    }

    //发起直播的数量
    val liveNumbersKV = Array("action:live_numbers")
    val liveNumbersFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveNumbersKV))
    //topic_tag:"美国大选;时事政治" //string型，topic或tag值，用英文分号（;）隔开
    val liveNumbersTagRes = liveNumbersFilter.map(line => getVal(line, "topic_tag"))
    val liveNumbersTagCount1 = liveNumbersTagRes.filter(tag => !"".equals(tag)).count()
    writer.println(dateInfo + "\tlive_numbers\ttopic_tag\t1\t" + liveNumbersTagCount1)
    val liveNumbersTagCount0 = liveNumbersTagRes.filter(tag => "".equals(tag)).count()
    writer.println(dateInfo + "\tlive_numbers\ttopic_tag\t0\t" + liveNumbersTagCount0)
    //type:1//int型，1=>用户添加，2=>cms后台添加,多路流直播类型记为cms后台添加,3=>openapi/sdk发起直播
    val liveNumbersTypeRes = liveNumbersFilter.map(line => (getVal(line, "type"), 1)).reduceByKey(_ + _)
    liveNumbersTypeRes.collect().foreach {
      res =>
        {
          val myType = res._1
          val myCount = res._2
          writer.println(dateInfo + "\tlive_numbers\ttype\t" + myType + "\t" + myCount)
        }
    }
    //with_gps:1//int型，是否开启定位：发起直播时记录，是为1否则为0
    val liveNumbersGpsRes = liveNumbersFilter.map(line => getVal(line, "with_gps"))
    val liveNumbersGpsCount1 = liveNumbersGpsRes.filter(gps => "1".equals(gps)).count()
    writer.println(dateInfo + "\tlive_numbers\twith_gps\t1\t" + liveNumbersGpsCount1)
    val liveNumbersGpsCount0 = liveNumbersGpsRes.filter(gps => "0".equals(gps)).count()
    writer.println(dateInfo + "\tlive_numbers\twith_gps\t0\t" + liveNumbersGpsCount0)
    //with_cover:0//int型，是否添加封面：发起直播时记录，是为1，否则为0
    val liveNumbersCoverRes = liveNumbersFilter.map(line => getVal(line, "with_cover"))
    val liveNumbersCoverCount1 = liveNumbersCoverRes.filter(cover => "1".equals(cover)).count()
    writer.println(dateInfo + "\tlive_numbers\twith_cover\t1\t" + liveNumbersCoverCount1)
    val liveNumbersCoverCount0 = liveNumbersCoverRes.filter(cover => "0".equals(cover)).count()
    writer.println(dateInfo + "\tlive_numbers\twith_cover\t0\t" + liveNumbersCoverCount0)
    //anti_shake:0//int型，是否开启了防抖：发起直播时记录，是为1，否则为0
    val liveNumbersShakeRes = liveNumbersFilter.map(line => getVal(line, "anti_shake"))
    val liveNumbersShakeCount1 = liveNumbersShakeRes.filter(shake => "1".equals(shake)).count()
    writer.println(dateInfo + "\tlive_numbers\tanti_shake\t1\t" + liveNumbersShakeCount1)
    val liveNumbersShakeCount0 = liveNumbersShakeRes.filter(shake => "0".equals(shake)).count()
    writer.println(dateInfo + "\tlive_numbers\tanti_shake\t0\t" + liveNumbersShakeCount0)

    //    //直播频道 
    val liveCidList = List("24hour_zb", "55_zb", "56_zb", "59_zb", "61_zb", "62_zb", "63_zb", "64_zb", "66_zb", "BTV_zb")
    //
    //    val viewKV = Array("action:view")
    //    val appChannelSwitchFilter1 = textFile.filter(line => SelectByKV.selectByKV(line, viewKV)).filter(line => liveCidList.contains(getCid(line)))
    //    val appChannelSwitchRes1 = appChannelSwitchFilter1.map(line => (getCid(line), getVal(line, "mid"))).filter(tuple => !tuple._2.equals(""))
    //    val appChannelPv1 = appChannelSwitchRes1.countByKey()
    //    var appChannelSwitchPvMap1: Map[String, Long] = Map()
    //    appChannelPv1.foreach {
    //      res =>
    //        {
    //          val myType = res._1
    //          val myCount = res._2
    //          appChannelSwitchPvMap1 += (myType -> myCount.toLong)
    //          //writer.println(dateInfo + "\tlive_duration\t" + myType + "\t" + myCount)
    //        }
    //    }
    //    val appChannelUv1 = appChannelSwitchRes1.groupByKey()
    //    appChannelUv1.collect().foreach {
    //      res =>
    //        {
    //          val myType = res._1
    //          val myPv = appChannelSwitchPvMap1(myType)
    //          var midSet: Set[String] = Set()
    //          res._2.foreach {
    //            mid => midSet += mid
    //          }
    //          val myUv = midSet.size
    //          writer.println(dateInfo + "\tapp_channel_switch\tview\t" + myType + "\t" + myPv + "\t" + myUv)
    //        }
    //    }
    //
    val clickKV = Array("action:click")
    val appChannelSwitchFilter2 = textFile.filter(line => SelectByKV.selectByKV(line, clickKV)).filter(line => liveCidList.contains(getCid(line)))
    val appChannelSwitchRes2 = appChannelSwitchFilter2.map(line => (getCid(line), getVal(line, "mid"))).filter(tuple => !tuple._2.equals(""))
    val appChannelPv2 = appChannelSwitchRes2.countByKey()
    var appChannelSwitchPvMap2: Map[String, Long] = Map()
    appChannelPv2.foreach {
      res =>
        {
          val myType: String = res._1
          val myCount: Long = res._2
          appChannelSwitchPvMap2 += (myType -> myCount)
        }
    }
    val appChannelUv2 = appChannelSwitchRes2.groupByKey()
    appChannelUv2.collect().foreach {
      res =>
        {
          val myType = res._1
          val myPv = appChannelSwitchPvMap2(myType)
          var midSet: Set[String] = Set()
          res._2.foreach {
            mid => midSet += mid
          }
          val myUv = midSet.size
          writer.println(dateInfo + "\tapp_channel_switch\tclick\t" + myType + "\t" + myPv + "\t" + myUv)
        }
    }

    //各类型态度气泡数
    val bubbleclickKV = Array("action:bubble_click")
    val bubbleclickFilter = textFile.filter(line => SelectByKV.selectByKV(line, bubbleclickKV))
    val bubbleclickBubble = bubbleclickFilter.map(line => (getVal(line, "bubble_type"), getVal(line, "mid"))).filter(tuple => !tuple._1.equals(""))
    val bubbleclickBubblePv = bubbleclickBubble.countByKey()
    var bubbleclickBubbleMap: Map[String, Long] = Map()
    bubbleclickBubblePv.foreach {
      res =>
        {
          val myType: String = res._1
          val myCount: Long = res._2
          bubbleclickBubbleMap += (myType -> myCount)
        }
    }
    val bubbleclickBubbleUv = bubbleclickBubble.groupByKey()
    bubbleclickBubbleUv.collect().foreach {
      res =>
        {
          val myType: String = res._1
          val myPv: Long = bubbleclickBubbleMap(myType)
          var midSet: Set[String] = Set()
          res._2.foreach {
            mid => midSet += mid
          }
          val myUv: Long = midSet.size
          writer.println(dateInfo + "\tbubble_click\tbubble_type\t" + myType + "\t" + myPv + "\t" + myUv)

        }
    }

    val bubbleclickScreen = bubbleclickFilter.map(line => (getVal(line, "screen_type"), getVal(line, "mid"))).filter(tuple => !tuple._1.equals(""))
    val bubbleclickScreenPv = bubbleclickScreen.countByKey()
    var bubbleclickScreenMap: Map[String, Long] = Map()
    bubbleclickScreenPv.foreach {
      res =>
        {
          val myType: String = res._1
          val myCount: Long = res._2
          bubbleclickScreenMap += (myType -> myCount)
        }
    }
    val bubbleclickScreenUv = bubbleclickScreen.groupByKey()
    bubbleclickScreenUv.collect().foreach {
      res =>
        {
          val myType: String = res._1
          val myPv: Long = bubbleclickScreenMap(myType)
          var midSet: Set[String] = Set()
          res._2.foreach {
            mid => midSet += mid
          }
          val myUv: Long = midSet.size
          writer.println(dateInfo + "\tbubble_click\tscreen_type\t" + myType + "\t" + myPv + "\t" + myUv)
        }
    }

    //列表中点击话题的次数/人数
    val liveTopicClickKV = Array("action:live_topic_click")
    val liveTopicClickFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveTopicClickKV))
    val liveTopicClickRes = liveTopicClickFilter.map(line => getVal(line, "mid"))
    val liveTopicClickPv = liveTopicClickRes.count()
    val liveTopicClickUv = liveTopicClickRes.distinct().count()
    writer.println(dateInfo + "\tlive_topic_click\t" + liveTopicClickPv + "\t" + liveTopicClickUv)

    //点击直播预告入口的次数/人数
    val livePreviewEntranceClickKV = Array("action:live_preview_entrance_click")
    val livePreviewEntranceClickFilter = textFile.filter(line => SelectByKV.selectByKV(line, livePreviewEntranceClickKV))
    val livePreviewEntranceClickRes = livePreviewEntranceClickFilter.map(line => getVal(line, "mid"))
    val livePreviewEntranceClickPv = livePreviewEntranceClickRes.count()
    val livePreviewEntranceClickUv = livePreviewEntranceClickRes.distinct().count()
    writer.println(dateInfo + "\tlive_preview_entrance_click\t" + livePreviewEntranceClickPv + "\t" + livePreviewEntranceClickUv)

    //只看主持人开启点击次数/人数
    val liveOnlyHostKV = Array("action:live_only_host")
    val liveOnlyHostFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveOnlyHostKV))
    val liveOnlyHostRes = liveOnlyHostFilter.map(line => getVal(line, "mid"))
    val liveOnlyHostPv = liveOnlyHostRes.count()
    val liveOnlyHostUv = liveOnlyHostRes.distinct().count()
    writer.println(dateInfo + "\tlive_only_host\t" + liveOnlyHostPv + "\t" + liveOnlyHostUv)

    //分享直播（总数和成功分开统计）的次数/人数
    val liveShareKV = Array("action:live_share")
    val liveShareFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveShareKV))
    //status:1 //int型，是否分享成功，成功为1，否则为0
    val liveShareRes1 = liveShareFilter.filter(line => "1".equals(getVal(line, "status"))).map(line => getVal(line, "mid"))
    val liveSharePv1 = liveShareRes1.count()
    val liveShareUv1 = liveShareRes1.distinct().count()
    writer.println(dateInfo + "\tlive_share\t1\t" + liveSharePv1 + "\t" + liveShareUv1)
    val liveShareRes0 = liveShareFilter.filter(line => "0".equals(getVal(line, "status"))).map(line => getVal(line, "mid"))
    val liveSharePv0 = liveShareRes0.count()
    val liveShareUv0 = liveShareRes0.distinct().count()
    writer.println(dateInfo + "\tlive_share\t0\t" + liveSharePv0 + "\t" + liveShareUv0)

    //「@」功能使用次数/人数：记录发出的消息中带有@用户名的条数/人数，没有发出的消息不用记录
    val liveAtKV = Array("action:live_at")
    val liveAtFilter = textFile.filter(line => SelectByKV.selectByKV(line, liveAtKV))
    val liveAtRes = liveAtFilter.map(line => getVal(line, "mid"))
    val liveAtPv = liveAtRes.count()
    val liveAtUv = liveAtRes.distinct().count()
    writer.println(dateInfo + "\tlive_at\t" + liveAtPv + "\t" + liveAtUv)

    //每场直播的美颜值：直播结束时记录
    val beautyKV = Array("action:live_with_beauty")
    val beautyFilter = textFile.filter(line => SelectByKV.selectByKV(line, beautyKV))
    //beauty:100 //int，美颜值
    val beauty = beautyFilter.map(line => (getVal(line, "beauty"), 1)).filter(tuple => !tuple._1.equals(""))
    val beautyPv = beauty.countByKey()
    beautyPv.foreach {
      res =>
        {
          val myType: String = res._1
          val myCount: Long = res._2
          writer.println(dateInfo + "\tlive_with_beauty\t" + myType + "\t" + myCount)
        }
    }

    writer.close
    sc.stop()
  }
}