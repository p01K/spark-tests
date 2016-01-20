/*
******* katsogr nested RDD testing ***********
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object NestedFilter1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("NestedRDD").setMaster(args(0))

    val sc = new SparkContext(conf)

    val textrdd = sc.textFile("/etc/passwd",1)

    val textrdd2 = sc.textFile("/etc/os-release",1)

    val textrdd3 = sc.textFile("/etc/hostname",1)

    val maprdd = textrdd.map( word =>
      textrdd2.map( word2 =>  word + word2)
        .filter( _.length > 3 )
        .collect() )

    val collectmap = maprdd.collect()

    collectmap.foreach( w => Console.println( w(0) ) )

    Console.println( s"End of task")

    sc.stop()
  }
}
