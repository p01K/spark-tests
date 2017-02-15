/*
Copyright 2016 katsogr

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

/*
Set of microbenchmarks for Apache Spark
@author katsogr
 * */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.util.Random


object Filter33{

  def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {
    val r = new Random(1117)

    val rdd = sc.parallelize(Array.tabulate(nelems)(i=>r.nextInt(10000))).repartition(npartitions).cache()

    rdd.count() //some warmup to enforce data caching

    val stats = Array.fill[Double](runs)(0d)

    for( i <- 0 until runs){
      val start  = System.currentTimeMillis()

      val result = rdd.filter( _%33 == 0).count()

      stats(i)   = System.currentTimeMillis()-start
    }
    return stats
  }
}

object Collect{

  def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {

    val rdd = sc.parallelize(Array.tabulate(nelems)(i=>1)).repartition(npartitions).cache()

    rdd.count() //some warmup to enforce caching

    val stats = Array.fill[Double](runs)(0d)

    for( i <- 0 until runs){
      val start  = System.currentTimeMillis()

      val result = rdd.collect()

      stats(i)   = System.currentTimeMillis()-start
    }

    return stats
  }
}

object ReducePlus{
  def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {

    val rdd = sc.parallelize(Array.tabulate(nelems)(i=>1)).repartition(npartitions).cache()

    rdd.count() //some warmup to enforce caching

    val stats = Array.fill[Double](runs)(0d)
    var result = 0
    for( i <- 0 until runs){
      val start  = System.currentTimeMillis()

      result    += rdd.reduce(_+_)

      stats(i)   = System.currentTimeMillis()-start
    }
    Console.println(s"result == $result")
    return stats
  }
}

object MeasureRepartition{
  def main(argv: Array[String]) = {
    val conf = new SparkConf().setAppName("Benchmark").setMaster("spark://imr40:7077")
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                              .set("spark.kryo.registrationRequired","false")
      .registerKryoClasses(Array(classOf[Array[Double]],classOf[Array[Int]]))
      .set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array.tabulate(5000000)(i=>1)).repartition(8192).cache()

    rdd.count() //some warmup to enforce caching

    for( i <- 0 until 5){
      val start  = System.currentTimeMillis()

      val result = rdd.repartition(32).repartition(8192).count()

      val time   = System.currentTimeMillis()-start
      Console.println(s"time ==  ${time/1000}")
    }
    // Console.println(s" == $result")
    // return stats
  }
}


object Cartesian{
  def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {
    val r = new Random(1117)

    val rdd = sc.parallelize(Array.tabulate(nelems)(i=>r.nextInt(10000)),npartitions).cache()
    val rdd2 = sc.parallelize(Array.tabulate(nelems)(i=>r.nextInt(10000)),npartitions).cache()

    rdd.count() //some warmup to enforce data caching
    rdd2.count()

    val stats = Array.fill[Double](runs)(0d)

    for( i <- 0 until runs){
      val start  = System.currentTimeMillis()

      val cart_rdd = rdd.cartesian(rdd2)

      val collectmap = cart_rdd.count()
      // assert(collectmap.size == nelems*nelems)

      // collectmap.foreach( Console.println(_) )
      // collectmap.foreach( elem => elem.foreach( Console.println(_) ) )

      stats(i)   = (System.currentTimeMillis()-start)/1000d
    }
    return stats
  }
}


object LongTail{

  val r = new Random(1117)

  def delay(nanos: Long):Unit = {
    val starttime = System.nanoTime()
    var elapsed   = 0L
    do {
      elapsed = System.nanoTime()-starttime
    }while(elapsed<nanos)
  }

  def samplep(): Int = { //90% return 10 10% return 10000
    val s = r.nextInt(100)
    if( s>=90 )
      50000000
    else
      100
  }

  def run(sc: SparkContext, npartitions: Int, runs: Int): Array[Double] = {

    val rdd = sc.parallelize(Array.tabulate(npartitions)(i=>1))
      .repartition(npartitions)
      .cache()

    rdd.count() //some warmup to enforce caching

    val stats = Array.fill[Double](runs)(0d)

    for( i <- 0 until runs){
      val start  = System.currentTimeMillis()

      val result = rdd.mapPartitions( p => {delay(samplep()); Array(1).toIterator}).collect()

      stats(i)   = System.currentTimeMillis()-start
    }

    return stats
  }

}

object WordCount {
  //val input = "data/wc/wc.big"
  val input = "data/wc/big2.in"

  def run(sc: SparkContext, npartitions: Int, runs: Int): Array[Double] = {

    val data = sc.textFile(input).repartition(npartitions).cache()

    val stats = Array.fill[Double](runs)(0d)

    data.count()

    for(i <- 0 until runs){
      val start = System.currentTimeMillis()

      val wc = data.flatMap(_.split(" ")).map( e => (e,1) ).reduceByKey(_+_).collect

      stats(i)   = System.currentTimeMillis()-start
    }
    return stats

  }

}


object Run{
  val NRDDS       = 1
  val NELEMENTS   = 5000000
  // val NITERATIONS = 1
  val RUNS        = 10
  val PARTITIONS  = 16
  val NSCHED      = 4

  def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
            """
      |Usage: Run [options]
      |
      | Options are:
      |   --master     <masterUrl>
      |   --nelems     <array elements>
      |   --runs       <job iterations>
      |   --partitions <partitions>
      |   --algo       <benchmark>
      |   --nrdds      <number of rdds>
      |   --dsched     <true || false>
      |   --nsched     <number of schedulers>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

  def parseArguments(args: Array[String]):(String,String,Boolean,Int,Int,Int,Int,Int) = {
    var nrdds  = NRDDS
    var nelems = NELEMENTS
    var runs   = RUNS
    var algo:String = null
    var npart  = PARTITIONS
    var dsched = false
    var nsched = NSCHED
    var master:String  = null

    System.err.println(s"options: ${args.toList.mkString(" ")}")

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--master")     :: value :: tail =>
          master = value
          argv   = tail
        case ("--nelems")     :: value :: tail =>
          nelems = value.toInt
          argv   = tail
        case ("--runs")       :: value :: tail =>
          runs   = value.toInt
          argv   = tail
        case ("--partitions") :: value :: tail =>
          npart  = value.toInt
          argv   = tail
        case ("--algo")       :: value :: tail =>
          algo   = value
          argv   = tail
        case ("--dist-sched") :: value :: tail =>
          // Console.println(s"value == $value")
          dsched = value match {
            case "true"  => true
            case "false" => false
          }
          argv   = tail
        case ("--nsched")     :: value :: tail =>
          nsched = value.toInt
          argv   = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }
    (master,algo,dsched,nsched,nelems,npart,runs,nrdds)
  }


  def main(args: Array[String]) = {

   val (master,algo,dsched,nsched,nelems,npart,runs,nrdds) = Run.parseArguments(args)

    val conf = new SparkConf().setAppName("Benchmark").setMaster(master)
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                              .set("spark.kryo.registrationRequired","false")
      .registerKryoClasses(Array(classOf[Array[Double]],classOf[Array[Int]]))
      .set("spark.executor.memory","4g")


    Console.println(s"conf: ${(algo,dsched,nsched,nelems,npart,runs,nrdds)}")

    if(dsched == true){
      conf.enableDistSchedulng(nsched)
    }

    val sc = new SparkContext(conf)

    val timearray = algo match {
      case "Collect"    =>
        Collect.run(sc,nelems,npart,runs)
      case "Filter33"   =>
        Filter33.run(sc,nelems,npart,runs)
      case "ReducePlus" =>
        ReducePlus.run(sc,nelems,npart,runs)
      case "LongTail"   =>
        LongTail.run(sc,npart,runs)
      case "WordCount"  =>
        WordCount.run(sc,npart,runs)
      case "Cartesian"  =>
        Cartesian.run(sc,nelems,npart,runs)
      case _            =>
        throw new Exception("Unknown algo")
    }

    Console.println(s"time: ${timearray.mkString(",")}")

  }
}
