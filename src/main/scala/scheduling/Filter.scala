import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.util.Random

//same as benchmark3 but with cachine enabled

// object Filter33{

//   def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {
//     val r = new Random(1117)

//     val rdd = sc.parallelize(Array.tabulate(nelems)(i=>r.nextInt(10000))).repartition(npartitions).cache()

//     rdd.count() //some warmup to enforce data caching

//     val stats = Array.fill[Double](runs)(0d)

//     for( i <- 0 until runs){
//       val start  = System.currentTimeMillis()

//       val result = rdd.filter( _%33 == 0).collect()

//       stats(i)   = System.currentTimeMillis()-start
//     }
//     return stats
//   }
// }

// object Collect{

//   def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {

//     val rdd = sc.parallelize(Array.tabulate(nelems)(i=>1)).repartition(npartitions).cache()

//     rdd.count() //some warmup to enforce caching

//     val stats = Array.fill[Double](runs)(0d)

//     for( i <- 0 until runs){
//       val start  = System.currentTimeMillis()

//       val result = rdd.collect()

//       stats(i)   = System.currentTimeMillis()-start
//     }

//     return stats
//   }
// }

// object ReducePlus{
//   def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {

//     val rdd = sc.parallelize(Array.tabulate(nelems)(i=>1)).repartition(npartitions).cache()

//     rdd.count() //some warmup to enforce caching

//     val stats = Array.fill[Double](runs)(0d)

//     for( i <- 0 until runs){
//       val start  = System.currentTimeMillis()

//       val result = rdd.reduce(_+_)

//       stats(i)   = System.currentTimeMillis()-start
//     }

//     return stats
//   }
// }

// object Run{
//   val NRDDS       = 1
//   val NELEMENTS   = 500000
//   // val NITERATIONS = 1
//   val RUNS        = 10
//   val PARTITIONS  = 16
//   val NSCHED      = 4

//   private def printUsageAndExit() = {
//     // scalastyle:off println
//     System.err.println(
//             """
//       |Usage: Run [options]
//       |
//       | Options are:
//       |   --master     <masterUrl>
//       |   --nelems     <array elements>
//       |   --runs       <job iterations>
//       |   --partitions <partitions>
//       |   --algo       <benchmark>
//       |   --nrdds      <number of rdds>
//       |   --dsched     <true || false>
//       |   --nsched     <number of schedulers>
//       |""".stripMargin)
//     // scalastyle:on println
//     System.exit(1)
//   }

//   def parseArguments(args: Array[String]):(String,String,Boolean,Int,Int,Int,Int,Int) = {
//     var nrdds  = NRDDS
//     var nelems = NELEMENTS
//     var runs   = RUNS
//     var algo:String = null
//     var npart  = PARTITIONS
//     var dsched = false
//     var nsched = NSCHED
//     var master:String  = null

//     var argv = args.toList
//     while (!argv.isEmpty) {
//       argv match {
//         case ("--master")     :: value :: tail =>
//           master = value
//           argv   = tail
//         case ("--nelems")     :: value :: tail =>
//           nelems = value.toInt
//           argv   = tail
//         case ("--runs")       :: value :: tail =>
//           runs   = value.toInt
//           argv   = tail
//         case ("--partitions") :: value :: tail =>
//           npart  = value.toInt
//           argv   = tail
//         case ("--algo")       :: value :: tail =>
//           algo   = value
//           argv   = tail
//         case ("--dist-sched") :: value :: tail =>
//           dsched = value.toBoolean
//           argv   = tail
//         case ("--nsched")     :: value :: tail =>
//           nsched = value.toInt
//           argv   = tail
//         case Nil =>
//         case tail =>
//           System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
//           printUsageAndExit()
//       }
//     }
//     (master,algo,dsched,nsched,nelems,npart,runs,nrdds)
//   }


//   def Main(args: Array[String]) = {

//    val (master,algo,dsched,nsched,nelems,npart,runs,nrdds) = Run.parseArguments(args)

//     val conf = new SparkConf().setAppName("Benchmark").setMaster(args(0))
//                               .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//                               .set("spark.kryo.registrationRequired","false")
//                               .registerKryoClasses(Array(classOf[Array[Double]],classOf[Array[Int]]))

//     Console.println(s"conf: ${args.mkString(",")}")

//     if(dsched == true){
//       conf.enableDistSchedulng(nsched)
//     }

//     val sc = new SparkContext(conf)

//     val timearray = algo match {
//       case "Collect"    =>
//         Collect.run(sc,nelems,npart,runs)
//       case "Filter33"   =>
//         Filter33.run(sc,nelems,npart,runs)
//       case "ReducePlus" =>
//         ReducePlus.run(sc,nelems,npart,runs)
//       case _            =>
//         throw new Exception("Unknown algo")
//     }

//     Console.println(s"time: ${timearray.mkString(",")}")

//   }
// }

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
