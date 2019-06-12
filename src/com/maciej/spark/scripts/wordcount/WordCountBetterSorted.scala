package com.maciej.spark.scripts.wordcount

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("C:\\Users\\Właściciel\\Desktop\\Moje_duperele\\IT\\Coursera_Udemy\\Apache_Spark2_with_Scala-HandsOn_z_dużymi_danymi\\SparkScala\\book.txt")
    
    // Split using a regular expression that extracts words; So we got one or more occurrences of NOT words (we're looking for special characters)
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word; we return RDD instead of map to be able to perform operations in distributed manner
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

