/**
 * bisecting <master> <input> <nNodes> <subIterations>
 *
 * divisive hierarchical clustering using bisecting k-means
 * assumes input is a text file, each row is a data point
 * given as numbers separated by spaces
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable.ArrayBuffer

case class Cluster(var key: Int, var center: Array[Double], var children: Option[List[Cluster]])

object Bisecting {

  /** recursively search cluster tree for desired key and insert children **/
  def insert(node: Cluster, key: Int, children: List[Cluster]) {
    if (node.key == key) {
      node.children = Some(children)
    } else {
      if (node.children.getOrElse(0) != 0) {
        insert(node.children.get(0),key,children)
        insert(node.children.get(1),key,children)
      }
    }
  }

  /** parse text into vectors **/
  def parseVector(line: String): Vector = {
    val vec = line.split(' ').map(_.toDouble)
    Vectors.dense(vec)
  }

  /** simple squared distance between two vectors **/
  def squaredDist(vec1: Vector, vec2: Vector) = {
    vec1.toArray.zip(vec2.toArray).map{ case (x, y) => math.pow(x - y, 2)}.sum
  }

  /** add two vectors **/
  def plus(vec1: Vector, vec2: Vector) = {
    Vectors.dense(vec1.toArray.zip(vec2.toArray).map{ case (x, y) => x + y})
  }

  /** divide a vector by a scalar **/
  def divide(vec1: Vector, s: Double) = {
    Vectors.dense(vec1.toArray.map (x => x / s))
  }

  /** closest point between a vector and an array of two vectors **/
  def closestPoint(p: Vector, centers: Array[Vector]): Int = {
    if (squaredDist(p, centers(0)) < squaredDist(p, centers(1))) {
      0
    }
    else {
      1
    }
  }

  /** split into two clusters using kmeans **/
  def split(cluster: RDD[Vector], subIterations: Int): Array[Vector] = {

    val convergeDist = 0.001
    var best = Double.PositiveInfinity
    var centersFinal = cluster.takeSample(false, 2, 1).toArray

    // try multiple splits and keep the best
    for (iter <- 0 until subIterations) {

      val centers = cluster.takeSample(false, 2, iter).toArray
      var tempDist = 1.0

      // do iterations of k-means till convergence
      while(tempDist > convergeDist) {
        val closest = cluster.map(p => (closestPoint(p, centers), (p, 1)))
        val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (plus(x1, x2), y1 + y2)}
        val newPoints = pointStats.map{pair => (pair._1, divide(pair._2._1, pair._2._2.toDouble))}.collectAsMap()
        tempDist = 0.0
        for (i <- 0 until 2) {
          tempDist += squaredDist(centers(i), newPoints(i))
        }
        for (newP <- newPoints) {
          centers(newP._1) = newP._2
        }
      }

      // check within-class similarity of clusters
      var totalDist = 0.0
      for (i <- 0 until 2) {
        totalDist += cluster.filter(x => closestPoint(x,centers)==i).map(x => squaredDist(x, centers(i))).reduce(_+_)
      }
      if (totalDist < best) {
        best = totalDist
        centersFinal = centers
      }
    }
    centersFinal
  }

  // def main(args: Array[String]) {

  //   if (args.length < 4) {
  //     System.err.println("Usage: bisecting <master> <input> <nNodes> <subIterations>")
  //     System.exit(1)
  //   }

  //   // collect arguments
  //   val master = args(0)
  //   val input = args(1)
  //   val nNodes = args(2).toDouble
  //   val subIterations = args(3).toInt

  //   // create spark context
  //   val sc = new SparkContext(master, "bisecting")
   
  //   // load raw data
  //   val data = sc.textFile(input).map(parseVector)

  //   println("starting")

  //   // create array buffer with first cluster and compute its center
  //   val clusters = ArrayBuffer((0,data))
  //   val n = data.count()
  //   val center = data.reduce{case (x, y) => plus(x, y)}.toArray.map(x => x / n)
  //   val tree = Cluster(0, center, None)
  //   var count = 1

  //   // start timer
  //   val startTime = System.nanoTime

  //   while (clusters.size < nNodes) {

  //     println(clusters.size.toString + " clusters, starting new iteration")

  //     // find largest cluster for splitting
  //     val ind = clusters.map(_._2.count()).view.zipWithIndex.max._2

  //     // split into 2 clusters using k-means
  //     val centers = split(clusters(ind)._2, subIterations) // find 2 cluster centers
  //     val cluster1 = clusters(ind)._2.filter(x => closestPoint(x, centers) == 0)
  //     val cluster2 = clusters(ind)._2.filter(x => closestPoint(x, centers) == 1)

  //     // get new indices
  //     val newInd1 = count
  //     val newInd2 = count + 1
  //     count += 2

  //     // update tree with results
  //     insert(tree, clusters(ind)._1, List(
  //       Cluster(newInd1, centers(0).toArray, None),
  //       Cluster(newInd2, centers(1).toArray, None)))

  //     // remove old cluster, add the 2 new ones
  //     clusters.remove(ind)
  //     clusters.append((newInd1, cluster1))
  //     clusters.append((newInd2, cluster2))

  //   }

  //   println("Bisecting took: "+(System.nanoTime-startTime)/1e9+"s")

  // }
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: bisecting <master> <input> <nNodes> <subIterations>")
      System.exit(1)
    }

    // collect arguments
    val master = args(0)
    val npoints = args(1).toInt
    val depth = args(2).toInt
    val iterations=scala.math.pow(2,depth).toInt
    val subIterations = args(3).toInt

    // create spark context
    val sc = new SparkContext(master, "bisecting")

    // load raw data
    val r = scala.util.Random
    val points = Array.tabulate(npoints)(i=>Vectors.dense(r.nextInt(1000).toDouble,r.nextInt(1000).toDouble))
    val data = sc.parallelize(points).cache()
    //
    //val data = sc.textFile(input).map(parseVector)

    println("starting")

    // create array buffer with first cluster and compute its center
    val clusters = ArrayBuffer((0,data))
    val n = data.count()
    val center = data.reduce{case (x, y) => plus(x, y)}.toArray.map(x => x / n)
    val tree = Cluster(0, center, None)
    var count = 1

    // start timer
    val start = System.currentTimeMillis()

    while (clusters.size < iterations) {

      println(clusters.size.toString + " clusters, starting new iteration")

      // find largest cluster for splitting
      val ind = clusters.map(_._2.count()).view.zipWithIndex.max._2

      // split into 2 clusters using k-means
      val centers = split(clusters(ind)._2, subIterations) // find 2 cluster centers
      val cluster1 = clusters(ind)._2.filter(x => closestPoint(x, centers) == 0)
      val cluster2 = clusters(ind)._2.filter(x => closestPoint(x, centers) == 1)

      // get new indices
      val newInd1 = count
      val newInd2 = count + 1
      count += 2

      // update tree with results
      insert(tree, clusters(ind)._1, List(
        Cluster(newInd1, centers(0).toArray, None),
        Cluster(newInd2, centers(1).toArray, None)))

      // remove old cluster, add the 2 new ones
      clusters.remove(ind)
      clusters.append((newInd1, cluster1))
      clusters.append((newInd2, cluster2))

    }

    val end = System.currentTimeMillis()

    Console.println(s"Total time elapsed : ${(end-start)/1000d} ms:clustersize ${clusters.size}")

    sc.stop()
    // val cls = clusters.map(_.count()).mkString(" ")
    // Console.println(cls)

    // println("Bisecting took: "+(System.nanoTime-startTime)/1e9+"s")

  }

 }
