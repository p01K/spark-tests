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
 * ****** katsogr NBODY(BarnesHut) simulation ***********
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark._
import scala.io.Source

case class Location
  (
    X : Double,
    Y : Double
  ){
  def > ( that : Location ) : Boolean = that match {
    case Location(thatx,thaty) => (thatx <= X && thaty <= Y)
  }
  def < ( that : Location ) : Boolean = that match {
    case Location(thatx,thaty) => (thatx >= X && thaty >= Y)
  }
  def dist( _loc : Location) = _loc match {
    case Location(_x,_y) =>  math.sqrt( (_x-X)*(_x-X)+(_y-Y)*(_y-Y) )
  }
  def avg( _loc : Location) = _loc match {
    case Location(x,y) => Location( (x+X)/2,(y+Y)/2)
  }
  def +( _loc : Location) = _loc match {
    case Location(x,y) => Location(X+x,Y+y)
  }
}

sealed trait Quad{
  def calcForces( p : Particle ) : Array[Double]
}

case class Particle
  (
    mass: Double,
    loc : Location,
    velocity: Double,
    id : Int
  ) extends Quad {

  def dist( p: Particle ) = p match {
    case Particle(_,ploc,_,_) => loc.dist(ploc)
  }

  def calcForces( p: Particle): Array[Double]= {
    Array( dist( p ) )
  }

  def toStr( ) = { s"id $id mass $mass velocity $velocity " }
}

case class QuadTree
  (
    mass : Double,
    loc  : Location,
    len  : Double,
    subtree: RDD[Quad]
  ) extends Quad with Logging
{
  def getMass() = mass

  def dist( p : Particle) = p match {  case Particle(_,_loc,_,_) => loc.dist(_loc) }

  def isFar( p : Particle , theta: Double  ) = {if( dist(p)/len < theta ) true else false }

  def force( p: Particle ) = p match {
    case Particle(massP,_,_,_)  => (mass*massP*BarnesHut.COSM)/dist(p)
  }

  def calcForces( p: Particle): Array[Double]= {
    if( isFar(p,BarnesHut.THETA) )  Array( force(p) )
    assert( subtree !=null )
    val arrays = subtree.map( child =>{ //child match {
      assert(child != null)
      child.calcForces( p )
    }).collect()
    Array.concat( arrays : _*)
  }

  def print() = { println("Printing tree "+subtree.collect() )}
}

case class Area
(
  startl : Location,
  finl   : Location
)
{
  def contains( p : Particle ) : Boolean = p match {
    case Particle(_,l,_,_) => (l > startl) && (l < finl)
  }

  def center() = { startl.avg(finl) }

  def split( ) : Array[Area] = {
    val center = startl.avg(finl)
    val (cX,cY) = center match { case Location(x,y) => (x,y) }
    val (sX,sY) = startl match { case Location(x,y) => (x,y) }
    val (eX,eY) = finl   match { case Location(x,y) => (x,y) }
    //split the area into 4 slices
    Array(  Area(startl,center) ,
            Area(center,finl)   ,
            Area( Location(cX,sY) , Location(eX,cY) ),
            Area( Location(sX,cY) , Location(cX,eY) )
    )
  }
}
object BarnesHut{

  val THETA=0.01
  val COSM=9.4

  def buildTree( sc:SparkContext , area : Area, particles : Array[Particle] ) : Quad = {
    assert( particles.length > 0)
    if( particles.length  == 1 ) particles.head
    else{
      val split = area.split()
      val children = split.map( a => {
        val subparticles  = particles.filter( a.contains(_) )
        if(subparticles.length > 0) buildTree(sc , a , subparticles)
        else null
      })
      val mass = particles.map( _.mass).reduce( _ + _ )
      val validNodes = children.filter( c => c != null )
      // validNodes.foreach( println( _ ) )
      QuadTree(mass,area.center() , 0 , sc.parallelize( validNodes)  )
    }
  }

  // @1 file with particles
  // @2 maximum length of Area
  //
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("BarnesHut").setMaster(args(0)).set("spark.executor.memory","8g")

    val sc = new SparkContext(conf)


    val datafile = sc.textFile( args(1) )
    val particles = datafile.map( line => line.split(" ") match {
      case Array(id , locationX , locationY , mass) =>
        Particle(mass.toDouble , Location(locationX.toDouble,locationY.toDouble),0.0,id.toInt)
    })
    val realpart = particles.collect()
    // realpart.foreach( p => println( "Particle : " + p.toStr() ) )
    val length = args(2).toInt

    val start  = System.currentTimeMillis()

    val tree = buildTree( sc , Area( Location(0,0) , Location( length,length )) ,realpart ).asInstanceOf[QuadTree]
    val forces = tree.calcForces( realpart.head )

    val time   = (System.currentTimeMillis()-start)/1000d

    Console.println(s"Time: $time")
    // forces.foreach( f => Console.println( f ) )
  }
}
