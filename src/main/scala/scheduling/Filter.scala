import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.util.Random

//same as benchmark3 but with cachine enabled
object Filter {

  def deviation(a: Array[Double]):Double = {
    val n = a.length
    val m = mean(a)
    val d = a.map(e => (e-m)*(e-m)).reduce(_+_)
    math.sqrt(d/n)
  }

  def mean(a: Array[Double]) = {
    val n = a.length
    a.reduce(_+_)/n
  }

  def main(args: Array[String]) {
    val NRDDS       = 1
    val NELEMENTS   = 500000
    val NITERATIONS = 1
    val RUNS        = 10

    if(args.length < 3){
      Console.println("Benchmark3 <master> <partitions> <distScheduling> <nschedulers>")
    }

    val (distScheduling,nschedulers) = args(2) match {
      case "true"  => (true,args(3).toInt)
      case "false" => (false,0)
      case _ => throw new Exception()
    }

    val conf = new SparkConf().setAppName("Benchmark").setMaster(args(0))

    val r = new Random(1117)

    if(distScheduling == true){
      conf.enableDistSchedulng(nschedulers)
    }

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array.tabulate(NELEMENTS)(i=>r.nextInt(10000))).repartition(args(1).toInt).cache()

    rdd.count()

    val stats = Array.fill[Double](RUNS)(0d)

    for( i <- 0 until RUNS){
      val start = System.currentTimeMillis()

      val result = rdd.filter( _%33 == 0).collect()

      stats(i)  = System.currentTimeMillis()-start

    }

    val timearray=stats

    val m = mean(timearray)/1000
    val d = deviation(timearray)/1000
    Console.println(s"stats: ${stats.mkString(",")} $m $d")

    sc.stop()

  }

}
