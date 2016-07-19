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

case class Pair(e1: Int, e2: Int) extends Serializable

object NestedCartesian{

  def parseArguments(args: Array[String]) = {
    var nelems = Run.NELEMENTS
    var runs   = Run.RUNS
    var npart  = Run.PARTITIONS
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
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          Run.printUsageAndExit()
      }
    }
    (master,nelems,npart,runs)
  }

  def run(sc: SparkContext, nelems: Int, npartitions: Int, runs: Int): Array[Double] = {
    val r = new Random(1117)

    val rdd = sc.parallelize(Array.tabulate(nelems)(i=>r.nextInt(10000)),npartitions).cache()

    val rdd2 = sc.parallelize(Array.tabulate(nelems)(i=>r.nextInt(10000)),npartitions).cache()

    rdd.count() //some warmup to enforce data caching
    rdd2.count()

    val stats = Array.fill[Double](runs)(0d)

    for( i <- 0 until runs){
      val start  = System.currentTimeMillis()

      val maprdd = rdd.mapBlock( p1 =>
        rdd.mapBlock( p2 =>
          p1.map(e1 =>
            p2.map(e2 => (e1,e2)
            )
          ).flatten
        ).collect()
      )
      val collectmap = maprdd.collect()
      assert(collectmap.size == nelems*nelems)
      // collectmap.foreach( Console.println(_) )
      // collectmap.foreach( elem => elem.foreach( Console.println(_) ) )

      stats(i)   = (System.currentTimeMillis()-start)/1000d
    }
    return stats
  }

  def main(args: Array[String]) {

    val (master,nelems,npart,runs) = NestedCartesian.parseArguments(args)

    val conf = new SparkConf()
      .setAppName("NestedRDD")
      .setMaster(master)
      .set("spark.executor.memory","4g")

      // .set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)
 
    val timearray = NestedCartesian.run(sc,nelems,npart,runs)

    Console.println(s"time: ${timearray.mkString(",")}")

    Console.println( s"End of task")

    sc.stop()
  }


}
