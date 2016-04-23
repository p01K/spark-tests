/*
@author katsogr
 * */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.parallel._
import scala.collection.parallel.mutable._

import org.apache.spark.rdd._
import org.apache.spark.rdd
import org.apache.spark.rdd.HierRDD._
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering._


class Cluster2(id: Int, iter:Int)
    extends Splittable[Vector] with Serializable{

  def id() = id
  var m : KMeansModel = null

  def this(id:Int, iter:Int, data:RDD[Vector]) {
    this(id,iter)
    m = KMeans.train(data, k=2, maxIterations=3)
  }

  def splitIndex(point: Vector) = m.predict(point)

  def contains(point: Vector) = (m.predict(point) == id)

  // def split(level:Int) : Array[Cluster2] = {
  //   m.clusterCenters.zipWithIndex.map{ case (c, idx) =>
  //       Cluster2(id + idx, c, data.filter(i => m.predict(i) == idx))
  //   }.toArray
  // }

  def split(level:Int,data:RDD[Vector]) : Array[Cluster2] = {
    if(m==null) m = KMeans.train(data, k=2, maxIterations=3)
    m.clusterCenters.zipWithIndex.map{ case (c, idx) =>
        new Cluster2(idx,iter,data)
    }.toArray
  }


  def splitPar(level:Int,data:RDD[Vector]) : ParArray[Cluster2] = {
    if(m==null) m = KMeans.train(data, k=2, maxIterations=3)
    m.clusterCenters.zipWithIndex.map{ case (c, idx) =>
        new Cluster2(idx,iter,data)

    }.toArray.par
  }
}

object HierarchicalKMeans {
  /** parse text into vectors **/
  def parseVector(line: String): Vector = {
    val vec = line.filter(c=>(c!='[' && c!=']')).split(',').map(_.toDouble)
    Vectors.dense(vec)
  }

  /** add two vectors **/
  def plus(vec1: Vector, vec2: Vector) = {
    Vectors.dense(vec1.toArray.zip(vec2.toArray).map { case (x, y) => x + y })
  }

  def main(args:Array[String]){

    if (args.length < 4) {
      System.err.println("Usage: bisecting <master> <nNodes> <subIterations> <rows>")
      System.exit(1)
    }

    val master = args(0)
    val npoints = args(1).toInt
    val maxdepth = args(2).toInt
    val subIterations = args(3).toInt
    val npartitions = args(4).toInt
    val input = args(5)

    val conf = new SparkConf().setAppName("HierarchicalKMeans").setMaster(master)

    val sc = new SparkContext(conf)

    val data = sc.textFile(input,npartitions)
      .map( parseVector(_) )
      .cache()

    val start = System.currentTimeMillis()

    val initcluster = new Cluster2(0, subIterations,data)
    val hierrdd = data.hierarchical2(initcluster)

    var split = hierrdd.split2()
    var depth = 1

    while(depth < maxdepth){
      val lstart = System.currentTimeMillis()
      split = split.flatMap( subrdd =>
        subrdd.split2())
      depth = depth + 1
      val ltime = (System.currentTimeMillis() - lstart )/1000
      Console.println(s"HKM iteration: $depth duration: $ltime")
    }

    val end = System.currentTimeMillis()

    Console.println(s"Total time elapsed : ${(end-start)/1000d} ms:clustersize ${split.size}")

    data.unpersist()
    val cls = split.map(_.count()).mkString(" ")
    Console.println(cls)

    sc.stop()
    //SANITY CHECK!!!
    // val exh = hierrdd.exhaust(2).map(_.collect())
    // exh.foreach( a => a.foreach( println(_))  )
  }

}
