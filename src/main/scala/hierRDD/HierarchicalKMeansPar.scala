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
@author pkatsogr
 * */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.parallel._
import scala.collection.parallel.mutable._

import org.apache.spark.rdd._
import org.apache.spark.rdd
import org.apache.spark.rdd.HierRDD._
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.clustering.KMeans

object HierarchicalKMeansPar {
  /** parse text into vectors **/
  def parseVector(line: String): Vector = {
    val vec = line.filter(c=>(c!='[' && c!=']')).split(',').map(_.toDouble)
    Vectors.dense(vec)
  }


  // def parseVector(line: String): Vector = {
  //   val vec = line.split(' ').map(_.toDouble)
  //   Vectors.dense(vec)
  // }

  /** add two vectors **/
  def plus(vec1: Vector, vec2: Vector) = {
    Vectors.dense(vec1.toArray.zip(vec2.toArray).map { case (x, y) => x + y })
  }

  def main(args:Array[String]){

    if (args.length < 4) {
      System.err.println("Usage: bisecting <master> <nNodes> <subIterations> <rows>")
      System.exit(1)
    }

    val master = args(0)
    val npoints = args(1).toInt
    val maxdepth = args(2).toInt
    val subIterations = args(3).toInt
    val npartitions = args(4).toInt
    val input = args(5)

    val conf = new SparkConf().setAppName("HierarchicalKMeansPar").setMaster(master)

    val sc = new SparkContext(conf)

    val data = sc.textFile(input,npartitions)
      .map( parseVector(_) )
      .cache()

    val start = System.currentTimeMillis()

    val initcluster = new Cluster2(0, subIterations)
    val hierrdd = data.hierarchical2(initcluster,false)

    var split = hierrdd.splitPar()
    var depth = 1

    while(depth < maxdepth){
      val lstart = System.currentTimeMillis()
      split = split.flatMap( subrdd => {
        subrdd.splitPar()
      })
      val ltime = (System.currentTimeMillis() - lstart )/1000
      Console.println(s"HKM iteration: $depth duration: $ltime")
      
    // val totalelems = split.map(subrdd => subrdd.count()).reduce(_+_)
    depth = depth + 1
    // Console.println(s"total elems $totalelems input elems ${data.distinct().count()}")	
    }

    val end = System.currentTimeMillis()

    Console.println(s"Total time elapsed ${(end-start)/1000d} ms:clustersize ${split.size}")
    val totalelems = split.map(subrdd => subrdd.count()).reduce(_+_)
    Console.println(s"total elems $totalelems input elems ${data.count()}")	
    data.unpersist()
    val cls = split.map(_.count()).mkString(" ")
    Console.println(cls)

    assert(totalelems == data.count(),"Split elements do not match the initial data")

    sc.stop()
  }

}
