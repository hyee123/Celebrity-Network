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

object cleaner {
  
  var actorCount : Int = 0
  var cartographer:Map[String, Int] = Map()
  type cup = (ArrayBuffer[String])
  type fiveTup = (Int, Int, Int, Int, Int)
def loadMap() : Map[String, Int] = 
{
      var actorsToInt:Map[String, Int] = Map()
      val lines = Source.fromFile("../tmdb-movies.csv").getLines()
      for (line <- lines) {
          
       
           val fields = line.split(",")
         if(fields.length  >= 21)
         {
             val title = fields(5)
             val cast = fields(6)
             val cast_fitted = cast.split("\\|")
           
             for(v <- cast_fitted)
             {
                  if(!(actorsToInt.contains(v)))
                  {
                      actorsToInt += (v -> actorCount)
                      actorCount += 1
                  }
             }
             
         }
           
      } 
    return actorsToInt
}
def parseLine(line: String) : Option[(String, fiveTup)] = {
   
    
     implicit val codec = Codec("UTF-8");
      codec.onMalformedInput(CodingErrorAction.REPLACE);
     codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
      val fields = line.split(",")
      if (fields.length < 21)
      {
        return None
      }
        val title = fields(5)
        val cast = fields(6)
        val cast_fitted = cast.split("\\|")
        var count :Int  = 0      
        if(cast_fitted.length == 5)
        {
           return Some(title, (cartographer(cast_fitted(0)), cartographer(cast_fitted(1)), cartographer(cast_fitted(2)), cartographer(cast_fitted(3)), cartographer(cast_fitted(4))))
          //return Some(title, (cast_fitted(0), cast_fitted(1), cast_fitted(2), cast_fitted(3), cast_fitted(4)))
        } else {
          return None
        }
      
}
def toAB ( line : fiveTup) : ArrayBuffer[Int]  =
{
 
      var a: ArrayBuffer[Int] = ArrayBuffer()
      var i : Int = 0
      a += line._1
      a += line._2
      a += line._3
      a += line._4
      a += line._5
      
      return a   
}
def facto ( a : ArrayBuffer[Int]) : Array[ArrayBuffer[Int]] = 
{
    
    var i : Int = 0
    var j : Int = 0
    var temp : ArrayBuffer[ArrayBuffer[Int]] = ArrayBuffer()
    
    for( i <- 0 to a.length -1 )
    {
      
      for(j <- 0 to a.length -1)
      {
           var cup : ArrayBuffer[Int] = ArrayBuffer()
           cup += a(i)
           cup += a(j)
           temp += cup
      }
      
    }
    return temp.toArray
}
def splitter( a : ArrayBuffer[Int] )  = 
{ 
   (a(0), a(1))
}
def combine ( thing : (Int, List[Int])) : List[Int] =
{
    return thing._2.::(thing._1)
}
def rmDup ( l : (Int,List[Int])) : (Int,List[Int]) =
{
   
    return (l._1,l._2.toSet.toList.sortWith(_<_))
  
  
 
}
def main(args: Array[String]) {
      
      //Initial Stage
      //=============================================
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sc = new SparkContext("local[*]", "degrees") 
      cartographer = loadMap()
      val data = sc.textFile("../tmdb-movies.csv")
      val am = new PrintWriter(new File("actorMap.txt" ))
      val pw = new PrintWriter(new File("socialNetData.txt" ))
      
      
      //Spark Stages
      //=============================================
      //title (actorA, actorB,...actorE)
      val twoCols = data.flatMap(parseLine)
      
      //(actorA, actorB,...actorE)
      val actorsTuple = twoCols.map( x => x._2)
      
      //map take in a the five tuple and return and arrayBuffer
      val ABactors = actorsTuple.map(toAB)
      
      val many = ABactors.flatMap(facto)
      
      val manyReformed = many.map(splitter)
      val groupActors = manyReformed.groupByKey()
      
       val sortedValues = groupActors.map(x => (x._1,x._2.toList))
     // val onething = sortedValues.map(combine)
      val duplicateRemoved = sortedValues.map(rmDup)
      val results = duplicateRemoved.collect()
      
      
      //Print Output Stage
      //====================================================
      results.foreach(println)
      
      //===Social Net Data
      for(result <- results){
       pw.write(result._1 + "|" )
        var c : Int = 0
        val end : Int = result._2.length
        for(item <- result._2)
        {
          if(c != end - 1)
          {
            pw.write(item + "|")
          } else
          {
            pw.write(item + "\n")
          }
          c += 1
        }
      }
      //====Actor Map data
      cartographer.foreach(keyvalue => am.write(keyvalue._2 + "|" + keyvalue._1 + "\n"))
      am.close
      print(actorCount)
      
      val s : String = "adfa,asdf,asd,fasd,fasdf,,asdf,asdf,,asdf"
      val f = s.split(",")
      print("======\n")
      print(f.length)
     // print(days.length)
      
  }
  
}