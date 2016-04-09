import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.util.Random

object WordCountTest {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("WordCount").setMaster(args(0))
    conf.enableDistSchedulng(4)

    val sc   = new SparkContext(conf)

    val data = sc.textFile(args(1)).flatMap(_.split(" ")).cache()

    data.count()

    
    val start = System.currentTimeMillis()
    val wc = data.map( e => (e,1) ).reduceByKey(_+_).collect
    val end   = System.currentTimeMillis()

    Console.println(s"time ${(end-start)/1000d}")

  }


}
