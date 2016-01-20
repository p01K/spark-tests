/*
******* katsogr nested RDD testing ***********
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object NestedMap2 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("NestedRDD").setMaster(args(0))

    val sc = new SparkContext( new SparkConf() )

    val textrdd = sc.textFile("/etc/passwd",1)

    val textrdd2 = sc.textFile("/etc/os-release",1)

    val textrdd3 = sc.textFile("/etc/hostname",1)

    val maprdd = textrdd.map( word => {
      val tmp = textrdd2.map( word2 =>
        textrdd3.map( word3 => word+word2+word3).collect() )
        tmp.collect()
    })
    val collectmap = maprdd.collect()

    collectmap.foreach( w =>
      w.foreach( w2 =>
        w2.foreach( w3 =>
          Console.println( w3 )))
    )

    Console.println( s"End of task")

    sc.stop()
  }
}
