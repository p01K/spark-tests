import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

/*
 cached elements
 map with delay
 count _+_
 */


object Benchmark6 {

  def main(args: Array[String]) {
    val NRDDS = 1
    val NELEMENTS = 500000
    val NITERATIONS = 1

    if(args.length < 4){
      Console.println("Benchmark4 <master> <partitions> <distScheduling> <delay>")
    }

    val distScheduling = args(2) match {
      case "true" => true
      case "false" => false
      case _ => false
    }

    val delay = args(3).toInt

    val conf = new SparkConf().setAppName("Benchmark6").setMaster(args(0))

    if(distScheduling == true){
      conf.enableDistSchedulng(16)
    }

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array.tabulate(NELEMENTS)(i=>1)).repartition(args(1).toInt).cache()

    // Console.println(s"Distributed scheduling enabled: $distScheduling")

    rdd.count()

    val start = System.currentTimeMillis()

    var sum = 0
    for(i<- 0 until 5){
      rdd.collect()
    }
    val stop = System.currentTimeMillis()

    Console.println(s"Total time elapsed : ${(stop-start)/5000d} ")

    sc.stop()

  }

}
