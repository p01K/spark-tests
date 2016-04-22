import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
//same as benchmark3 but with cachine enabled
object Benchmark5 {

  // def Benchmark6

  def main(args: Array[String]) {
    val NRDDS = 1
    val NELEMENTS = 500000
    val NITERATIONS = 1

    if(args.length < 3){
      Console.println("Benchmark3 <master> <partitions> <distScheduling> <nschedulers>")
    }

    val (distScheduling,nschedulers) = args(2) match {
      case "true"  => (true,args(3).toInt)
      case "false" => (false,0)
      case _ => throw new Exception()
    }

    val conf = new SparkConf().setAppName("Benchmark").setMaster(args(0))

    if(distScheduling == true){
      conf.enableDistSchedulng(nschedulers)
    }

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array.tabulate(NELEMENTS)(i=>1)).repartition(args(1).toInt).cache()

    rdd.count()

    val start = System.currentTimeMillis()

    var sum = 0
    for( i <- 0 until 10){
      sum += rdd.reduce(_+_)
    }
    val stop = System.currentTimeMillis()

    Console.println(s"Total time elapsed : ${(stop-start)/5000d} seconds Sum=${sum/5}")

    sc.stop()

  }

}
