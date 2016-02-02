
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.clustering.KMeans

import scala.collection.mutable.ArrayBuffer

object BisectingMLlib {

  /** recursively search cluster tree for desired key and insert children **/
  def insert(node: Cluster, key: Int, children: List[Cluster]) {
    if (node.key == key) {
      node.children = Some(children)
    } else {
      if (node.children.getOrElse(0) != 0) {
        insert(node.children.get(0), key, children)
        insert(node.children.get(1), key, children)
      }
    }
  }

  def parseVector(line: String): Vector = {
    val vec = line.filter(c=>(c!='[' && c!=']')).split(',').map(_.toDouble)
    Vectors.dense(vec)
  }


  /** parse text into vectors **/
  // def parseVector(line: String): Vector = {
  //   val vec = line.split(' ').map(_.toDouble)
  //   Vectors.dense(vec)
  // }

  /** simple squared distance between two vectors **/
  def squaredDist(vec1: Vector, vec2: Vector) = {
    vec1.toArray.zip(vec2.toArray).map { case (x, y) => math.pow(x - y, 2) }.sum
  }

  /** add two vectors **/
  def plus(vec1: Vector, vec2: Vector) = {
    Vectors.dense(vec1.toArray.zip(vec2.toArray).map { case (x, y) => x + y })
  }

  /** divide a vector by a scalar **/
  def divide(vec1: Vector, s: Double) = {
    Vectors.dense(vec1.toArray.map(x => x / s))
  }
  def closestPoint(p: Vector, centers: Array[Vector]): Int = {
    if (squaredDist(p, centers(0)) < squaredDist(p, centers(1))) {
      0
    } else {
      1
    }
  }

  /** split into two clusters using kmeans **/
  def split(cluster: RDD[Vector], subIterations: Int) = {

    val m = KMeans.train(cluster, k = 2, maxIterations = subIterations)
    m
  }

  def main(args: Array[String]) {
    
    if (args.length < 4) {
      System.err.println("Usage: bisecting <master> <nNodes> <subIterations> <rows>")
      System.exit(1)
    }

    // collect arguments
    val master = args(0)
    val npoints = args(1).toInt
    val depth = args(2).toInt+1
    val subIterations = args(3).toInt
    val rows = args(3).toInt
    val npartitions = args(4).toInt
    
    val input = args(5)

    val dim = 2

    val appName = s"${this.getClass().getSimpleName},nNodes,${depth},rows:${rows}"
    // create spark context
    val sc = new SparkContext(master, appName)

    val data = sc.textFile(input,npartitions)
      .map( parseVector(_) )
      .cache()

    println("starting")
    val clusters = ArrayBuffer(data)
    val n = data.count()
    val center = data.reduce { case (x, y) => plus(x, y) }.toArray.map(x => x / n)
    val tree = Cluster(0, center, None)
    var count = 1
    
    val start = System.currentTimeMillis()

    var ind = 0
    var splits = Seq(data)
    while(ind < depth){
      val lstart = System.currentTimeMillis()

      splits = splits.flatMap( d => {
        val model = split(d,subIterations)
        model.clusterCenters.zipWithIndex.map{
          case(c,idx) => d.filter(x=>model.predict(x)==idx)
        }
      })
      ind = ind + 1
      val ltime = (System.currentTimeMillis() - lstart )/1000
      //val totalelems = splits.map(subrdd => subrdd.count()).reduce(_+_)

      //Console.println(s"HKM iteration: $depth duration: $ltime splitsSize $totalelems")

    }

    val end = System.currentTimeMillis()

    Console.println(s"Total time elapsed : ${(end-start)/1000d} ms:clustersize ${splits.size}")
    data.unpersist()
    val cls = splits.map(_.count()).mkString(" ")
    Console.println(cls)

    sc.stop()
  }
}
