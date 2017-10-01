package com.cloudera.oryx.lambda_app.graphs

import com.cloudera.oryx.lambda_app.message_objects.{DeviceMessage, DeviceMessageManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.util.StatCounter

object GraphManager {

  def parseDeviceMessage(line: String): DeviceMessage = {
    new DeviceMessageManager().getDeviceMessageFromDeviceMessageJson(line)
  }

  val conf = new SparkConf().setAppName("SparkGraphXDeviceMessage")
  val sc = new SparkContext(conf)
  //Create RDD with the January 2014 data
  val textRDD = sc.textFile("/user/user01/data/rita2014jan.csv")

  val deviceMessagesRDD = textRDD.map(parseDeviceMessage).cache()

  val devices = deviceMessagesRDD.map(deviceMessage => deviceMessage.getDeviceId).distinct
  val messageTypes = deviceMessagesRDD.map(deviceMessage => deviceMessage.getSubtopic).distinct

  // Defining a default vertex called for undefined devices
  val undefined = "undefined"

  /*
  val subtopics = deviceMessagesRDD.map(deviceMessage => (deviceMessage.getDeviceId, deviceMessage.getSubtopic)).distinct
  subtopics.cache

  // AirportID is numerical - Mapping airport ID to the 3-letter code
  val deviceMap = devices.collect.toList.toMap

  //airportMap: scala.collection.immutable.Map[Long,String] = Map(13024 -> LMT, 10785 -> BTV, 14574 -> ROA, 14057 -> PDX, 13933 -> ORH, 11898 -> GFK, 14709 -> SCC, 15380 -> TVC,

  // Defining the subtopics as edges
  val edges = subtopics.map { case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance) }

  // Defining the Graph
  val graph = Graph(devices, edges, undefined)

  // Number of airports
  val numairports = graph.numVertices

  graph.vertices.take(2)

  graph.edges.take(2)

  graph.edges.filter { case (Edge(org_id, dest_id, distance)) => distance > 1000 }.take(3)
  // res9: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(10140,10397,1269), Edge(10140,10821,1670), Edge(10140,12264,1628))

  // Number of subtopics
  val numroutes = graph.numEdges

  // The EdgeTriplet class extends the Edge class by adding the srcAttr and dstAttr members which contain the source and destination properties respectively.
  graph.triplets.take(3).foreach(println)

  // Define a reduce operation to compute the highest degree vertex
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  // Compute the max degrees
  val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
  // maxInDegree: (org.apache.spark.graphx.VertexId, Int) = (10397,152)
  val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
  // maxOutDegree: (org.apache.spark.graphx.VertexId, Int) = (10397,153)
  val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)
  // maxDegrees: (org.apache.spark.graphx.VertexId, Int) = (10397,305)

  // we can compute the in-degree of each vertex (defined in GraphOps) by the following:
  // which airport has the most incoming flights?
  graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (deviceMap(x._1), x._2))
  //res46: Array[(String, Int)] = Array((ATL,152), (ORD,145), (DFW,143), (DEN,132), (IAH,107), (MSP,96), (LAX,82), (EWR,82), (DTW,81), (SLC,80),
  graph.outDegrees.join(devices).sortBy(_._2._1, ascending = false).take(1)
  val maxout = graph.outDegrees.join(devices).sortBy(_._2._1, ascending = false).take(3)
  //res46: Array[(String, Int)] = Array((ATL,152), (ORD,145), (DFW,143), (DEN,132), (IAH,107), (MSP,96), (LAX,82), (EWR,82), (DTW,81), (SLC,80),
  val maxIncoming = graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (deviceMap(x._1), x._2)).take(3)
  maxIncoming.foreach(println)


  maxout.foreach(println)

  val maxOutgoing = graph.outDegrees.collect.sortWith(_._2 > _._2).map(x => (deviceMap(x._1), x._2)).take(3)
  maxOutgoing.foreach(println)

  // What are the top 10 flights from airport to airport?
  graph.triplets.sortBy(_.attr, ascending = false).map(triplet =>
    "There were " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10)

  val sourceId: VertexId = 13024
  // 50 + distance / 20
  graph.edges.filter { case (Edge(org_id, dest_id, distance)) => distance > 1000 }.take(3)

  val gg = graph.mapEdges(e => 50.toDouble + e.attr.toDouble / 20)
  val initialGraph = gg.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
  */

  // TODO: number of messages sent
  // TODO: Average message type
  // TODO: least common message type
  // TODO: most common message type

  // TODO: number of messages sent by message type
  // TODO: Average message value by message type
  // TODO: least common message value by message type
  // TODO: most common message value by message type

  // TODO: number of messages sent by each device, by message type
  // TODO: Average message value by each device, by message type
  // TODO: least common message value by each device, by message type
  // TODO: most common message value by each device, by message type

  // TODO: number of devices in each K-mean
  // TODO: which devices are in each k-mean

}