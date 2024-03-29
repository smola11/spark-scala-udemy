package com.maciej.spark.scripts.popularmovies

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesNicer {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // We have foreign movies in the file
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8") // implicit = domyslny, niejawny;
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("C:\\Users\\Właściciel\\Desktop\\Moje_duperele\\IT\\Coursera_Udemy\\Apache_Spark2_with_Scala-HandsOn_z_dużymi_danymi\\ml-100k\\u.item").getLines()
     for (line <- lines) {
       val fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     movieNames
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")

    // Create a BROADCAST VARIABLE of our ID -> movie name map; this map will be available to our entire cluster (broadcast to every node)
    val nameDict = sc.broadcast(loadMovieNames())
    
    // Read in each rating line
    val lines = sc.textFile("C:\\Users\\Właściciel\\Desktop\\Moje_duperele\\IT\\Coursera_Udemy\\Apache_Spark2_with_Scala-HandsOn_z_dużymi_danymi\\ml-100k\\u.data")
    
    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    
    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    
    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    // Collect and print results
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
  }
  
}

