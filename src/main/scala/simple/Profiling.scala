/*

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
import com.github.nscala_time.time.Imports._
import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat

case class Profile(val region: Int, val week_id: Int, is_we: Boolean, day_time: Int, val count: Double){
  def normalize(): Profile = {
    val div: Double = if( is_we == true ) 2d else 5d

    Profile(region,week_id,is_we,day_time,count / div)
  }

  def plus(other: Profile){
    Profile(region, week_id, is_we, day_time, count+other.count)
  }
}

object CDR {
  def from_string(row: String, field2col: Map[String,Int]) = {
    val split      = row.split(";")
    val user_id    = split( field2col("user_id") ).toInt
    val start_cell = split( field2col("start_cell") ).toInt
    val end_cell   = split( field2col("end_cell") ).toInt
    val date       = string_to_date( split( field2col("date") ) )
    val time       = split( field2col("date") ).toInt

    new CDR(user_id, start_cell, end_cell, date, time)
  }

  def string_to_date(str: String): DateTime = {
    val datePattern = "yyyy-MM-dd"
    val timePattern = "HHmmss"
    val datetimeDelim = ":"
    val dateFormat = DateTimeFormat.forPattern(datePattern)
    dateFormat.parseDateTime(str)
  }

}

class CDR(val user_id: Int, start_cell: Int, end_cell: Int, date: DateTime, val time: Int){

  def is_we(): Boolean = { Range(0,6).contains( date.dayOfWeek().get()) }

  def day_of_week(): Int = { date.dayOfWeek().get() }

  def year(): Int = { date.year().get() }

  def region(cell2region: Map[Int,Int]): Int = { cell2region(start_cell) }

  def valid_region(cell2region: Map[Int,Int]): Boolean = { cell2region.contains(start_cell) }

  def day_time(): Int = { time }

  def week(): Int = { date.weekOfWeekyear().get() }

}

object UserProfiling {

  // def array_carretto(profiles: Array[Profile], weeks: Array[Int], uid: Int): Array[(Int,Int,Array[Double])] = {
  //   val munic = profiles.map(_.region).distinct()

  //   munic.map( m => {
  //     val obs = profiles.map(_.region == munic)

  //     val sums = obs.groupBy(_.week_id).map{ (i,v) => }

  //     val carr = Array.tabulare(weeks.size()*6)
  //   })
  // }

  def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
            """
      |Usage: Run [options]
      |
      | Options are:
      |   --master     <masterUrl>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

  def parseArguments(args: Array[String]) = {
    var master :String  = null
    var spatial:String  = null
    var field  :String  = null

    System.err.println(s"options: ${args.toList.mkString(" ")}")

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--master")      :: value :: tail =>
          master = value
          argv   = tail
        case ("--spatial_div") :: value :: tail =>
          spatial = value
          argv   = tail
        case ("--field") :: value :: tail =>
          field = value
          argv   = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }
    (master,spatial,field)
  }


  def main(args: Array[String]) = {

    val (master,spatial_div,field_file) = parseArguments(args)

    val cell2munic = Source.fromFile(spatial_div).getLines().map( line => {
      val split = line.split(";")
      (split(0).toInt,split(1).trim.replace("\n","").toInt)
    }).toMap

    val mutfield2col = Source.fromFile(field_file).getLines().map( line => {
      val split = line.split(",")
      (split(0),split(1).trim.toInt)
    }).toMap

    val conf = new SparkConf().setAppName("Benchmark").setMaster(master)
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                              .set("spark.kryo.registrationRequired","false")
      .registerKryoClasses(Array(classOf[Array[Double]],classOf[Array[Int]]))
      // .set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

    val files = "lalala"

    val data = sc.textFile("files")
      .map( line => CDR.from_string(line,mutfield2col) )
      .filter( cdr => cdr.valid_region(cell2munic) )

    val weeks = Array.tabulate(4)(i => i) // hardcoded for now

    val wgroups = weeks.zipWithIndex.filter{ case (w,i) => i%4==0 }

    wgroups.foreach{ case(w,i) =>
      val slice = weeks.slice(i,i+4)
      val profiles = data.filter( cdr => slice.contains(cdr.week()) ).map( cdr =>
        ((cdr.user_id,cdr.region(cell2munic),slice.indexOf(cdr.week()),cdr.is_we(),cdr.day_time()),1)
      ).reduceByKey{ case(v1,v2) => v1+v2 }
        // .map()
        // .reducrByKey{ case(v1,v2) => v1++v2} //concat
    }
    // val weeks = ....
    // group weeks


    // Console.println(s"conf: ${(algo,dsched,nsched,nelems,npart,runs,nrdds)}")

  }

}
