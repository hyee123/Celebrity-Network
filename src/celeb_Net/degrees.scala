package celeb_Net

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer



import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import java.io._

object degrees {
  
  val startID = 468 //Chloe Grace Moretz
  val targetID = 12796//input person
  var globTrail : ArrayBuffer[Int] = ArrayBuffer()
  
  //Spark safe variable counter
  var isFound:Option[LongAccumulator] = None
  // celeb id, depth, distance
  type data = (Array[Int], Int, Int, ArrayBuffer[Int])
  type node = (Int, data)
  
  def nameMap() : Map[Int, String] = {
    
    var temp: Map[Int, String] = Map()
   
    val lines = Source.fromFile("../actorMap.txt").getLines()
    for (line <- lines) {
       var f = line.split("\\|")
       if (f.length > 1) {
        temp += (f(0).toInt -> f(1))
       }
    }
    
    return temp
    
    
    
  }
  
  def initialize(sc:SparkContext): RDD[node] = {
    
    //convert text file RDD
    val inputFile = sc.textFile("../socialNetData.txt")
    return inputFile.map(createNodes)
  }
   def createNodes(l : String): node = {
     
     //val f = l.split("\\s+")
     val f = l.split("\\|")
     val sid : Int = f(0).toInt
     var temp: ArrayBuffer[Int] = ArrayBuffer()
     //var trail : ArrayBuffer[Int] = ArrayBuffer() 
     
     for(i <- 1 to (f.length-1))
     {
       temp += f(i).toInt
     }
     
     // 0 = unknown     1 = ready      2 = searched
     
     var depth : Int = 0
     var distance : Int = 6969
     
     if(sid == startID)
     {
       depth = 1
       distance = 0
     }
     return (sid, (temp.toArray,depth,distance,ArrayBuffer()))
     
   }
  
  def searchMapper(n : node)  : Array[node] =
  {
    var temp: ArrayBuffer[node] = ArrayBuffer()
    var trail : ArrayBuffer[Int] = ArrayBuffer()
    
    val sid = n._1
    val dt: data = n._2
    val ttt = dt._4
    
    val celeb_list: Array[Int] = dt._1
    var deep = dt._2
    val dis = dt._3
   
    if (deep == 1)
    {
      trail ++= dt._4
      trail += sid
      for(celeb <- celeb_list)
      {
        val newId = celeb
        var newDeep = 1
        val newDist = dis + 1
        
        if( celeb == targetID)
        {
          //indicate something was found
          if (isFound.isDefined) {
            isFound.get.add(1)
            newDeep = 1000
            
          }
        }
        //val tyt = ttt += sid
       val newNode : node = (newId, (Array(), newDeep, newDist,trail))
       temp += newNode
      }
      deep = 2
    }
    
    
    //return the original too
    val sameNode : node = (sid, (celeb_list,deep,dis, dt._4))
    temp += sameNode
    
    
    return temp.toArray
    
  }
  def searchReducer( d1 : data , d2 : data)  : data =
  {
    //create basic "data" set up
    var cel_list : ArrayBuffer[Int] = ArrayBuffer()
    var setDist : Int = 9999
    var setDeep : Int = 0
    var trail : ArrayBuffer[Int] = ArrayBuffer()
    //retreive data from both ends
    
    //d1 data
    val d1_list: Array[Int] = d1._1
    val d1_deep: Int = d1._2
    val d1_dist : Int = d1._3
    
    //d2 data
    val d2_list: Array[Int] = d2._1
    val d2_deep: Int = d2._2
    val d2_dist : Int = d2._3
    
    //add both arrays to one
    if (d1_list.length > 0) {
      cel_list ++= d1_list
    }
    if (d2_list.length > 0) {
      cel_list ++= d2_list
    }
    
    
    //take deepest depth
    if( d1_deep > d2_deep)
    {
      setDeep = d1_deep
    } else
    {
      setDeep = d2_deep
    }
    
    
    //take shortest dist
    if( d1_dist > d2_dist)
    {
      //set the trail
      setDist = d2_dist 
      trail = d2._4
    }else
    {
       //set the trail
      trail = d1._4
       setDist = d1_dist  
    }
    
    
    //return that data
    
  return (cel_list.toArray, setDeep, setDist, trail)  
    
    
  }
  
  //so mainstream
  def main(args: Array[String]) {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Degree")
    val nameDic = nameMap()
    isFound = Some(sc.longAccumulator("found"))
    
    var hq = initialize(sc)
    var i: Int = 0
    
    for(i <- 1 to 10)
    {
      
      val mapping = hq.flatMap(searchMapper)
      mapping.cache()
      println("Searching " + mapping.count() + " values..." + "Layer:" + i)
      hq = mapping.reduceByKey(searchReducer)
      if(isFound.isDefined)
      {
        if(isFound.get.value > 0)
        {
          val f = mapping.filter(x => x._2._2 == 1000)
          val f2 = f.collect()
          println("Connection Found... Bacon Number:" + i)
         // println(f2(0)._2._4)
          println("Connection Path:")
          for( i <- f2(0)._2._4)
          {
            //println(nameDic(i))
            println(nameDic(i))
          }
          println(nameDic(targetID))
          return
        }
      }
    // hq = mapping.reduceByKey(searchReducer)
      
    }
    
    
  
  }
      
      
}