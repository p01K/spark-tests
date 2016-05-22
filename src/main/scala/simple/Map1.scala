/*
******* p01K nested RDD testing ***********
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object Map1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("NestedRDD").setMaster(args(0))

    val sc = new SparkContext(conf)

    val textrdd = sc.textFile("/etc/passwd")

    // val maprdd = textrdd.map( word => textrdd.map(word2 =>  word+word2).collect() )

    val collectmap = textrdd.collect()

    collectmap.foreach( Console.println(_) )

    Console.println( s"End of task")

    sc.stop()
  }

}
