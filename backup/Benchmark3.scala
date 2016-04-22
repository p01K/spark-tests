import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object Benchmark3 {

  def main(args: Array[String]) {
    val NRDDS = 1
    val NELEMENTS = 500000
    val NITERATIONS = 1

    if(args.length < 3){
      Console.println("Benchmark3 <master> <partitions> <distScheduling>")
    }

    val distScheduling = args(2) match {
      case "true" => true
      case "false" => false
      case _ => false
    }

    val conf = new SparkConf().setAppName("Benchmark1").setMaster(args(0))

    if(distScheduling == true){
      conf.enableDistSchedulng(16)
    }

    val sc = new SparkContext(conf)

    val arrayrdd = Array.tabulate(NRDDS)(
      l => sc.parallelize(Array.tabulate(NELEMENTS)(i=>i)).repartition(args(1).toInt)
    )


    Console.println(s"Distributed scheduling enabled: $distScheduling")

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
