import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object Benchmark3 {

  def main(args: Array[String]) {
    val NRDDS = 40
    val NELEMENTS = 100000
    val NITERATIONS = 1

    val conf = new SparkConf().setAppName("Benchmark1")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val arrayrdd = Array.tabulate(NRDDS)(
	l => sc.parallelize(Array.tabulate(NELEMENTS)(i=>i)).repartition(args(1).toInt)
    )

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

