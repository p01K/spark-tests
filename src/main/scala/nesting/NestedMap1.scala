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
******* katsogr nested RDD testing ***********
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object NestedMap1 {

  def main(args: Array[String]){

    val conf = new SparkConf()
      .setAppName("NestedRDD")
      .setMaster(args(0))
      .set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

    val textrdd = sc.textFile("nesting.dat",1)

    val maprdd = textrdd.map( word => textrdd.map(word2 =>  word+word2).collect() )

    val collectmap = maprdd.collect()

    collectmap.foreach( str => Console.println(s"${str.mkString(",")}") )

    Console.println( s"End of task")

    sc.stop()
  }

}
