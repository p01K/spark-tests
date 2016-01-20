/*
******* katsogr nested RDD testing ***********
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object NestedMapReduce1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("NestedRDD").setMaster(args(0))

    val sc = new SparkContext(conf)

    val textrdd = sc.textFile("/etc/passwd")

    val textrdd2 = sc.textFile("/etc/os-release")

    val maprdd = textrdd.map( word => {
      textrdd2.map(word2  =>  word.length+word2.length).reduce( (s1,s2) => s1+s2)
    }
    )

    val collectmap = maprdd.collect()

    collectmap.foreach( Console.println(_)  )

    Console.println( s"End of task")

    sc.stop()
  }

}
