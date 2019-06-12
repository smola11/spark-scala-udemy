package com.maciej.spark.scripts.wordcount

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("C:\\Users\\Właściciel\\Desktop\\Moje_duperele\\IT\\Coursera_Udemy\\Apache_Spark2_with_Scala-HandsOn_z_dużymi_danymi\\SparkScala\\book.txt")
    
    // Split using a regular expression that extracts words; So we got one or more occurrences of NOT words (we're looking for special characters)
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()
    
    // Print the results
    wordCounts.foreach(println)
  }
  
}

