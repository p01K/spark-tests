import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object Benchmark1 {

  def main(args: Array[String]) {
    val NRDDS = 100
    val NELEMENTS = 100000
    val NITERATIONS = 100

    val conf = new SparkConf().setAppName("Benchmark1")
      .setMaster(args(0))
    // .set("spark.core.connection.ack.wait.timeout","600")
    // .set("spark.akka.frameSize","50")

    val sc = new SparkContext(conf)

    val arrayrdd = Array.tabulate(NRDDS)(l=>sc.parallelize(Array.tabulate(NELEMENTS)(i=>i)))

    val start = System.currentTimeMillis()

    val arrayrddpar = arrayrdd.par

    val start2 = System.currentTimeMillis()

    for(i <- 0 until NITERATIONS){
      val sum = arrayrddpar.map(rdd => rdd.reduce(_+_))
      // val sum = arrayrdd.reduce(_+_)
      // totalsum = totalsum + sum
    }

    val stop = System.currentTimeMillis()

    Console.println(s"Total time elapsed : ${(stop-start)/1000d}  seconds")

    sc.stop()

  }

}
