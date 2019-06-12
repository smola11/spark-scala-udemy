package com.maciej.spark.scripts.wordcount

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    // Read each line of my book into an RDD
    val input = sc.textFile("C:\\Users\\Właściciel\\Desktop\\Moje_duperele\\IT\\Coursera_Udemy\\Apache_Spark2_with_Scala-HandsOn_z_dużymi_danymi\\SparkScala\\book.txt")

    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))

    // Count up the occurrences of each word
    val wordCounts = words.countByValue()

    // Print the results.
    wordCounts.foreach(println)
  }

}

