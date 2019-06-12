package com.maciej.spark.scripts

import org.apache.log4j._
import org.apache.spark._

/** Compute the average number of friends by age in a social network. */
object PurchaseByCustomer {

  /** A function that splits a line of input into (customerId, amountSpent) tuples. */
  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amountSpent = fields(2).toFloat
    (customerId, amountSpent)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")

    val lines = sc.textFile("C:\\Users\\Właściciel\\Desktop\\Moje_duperele\\IT\\Coursera_Udemy\\Apache_Spark2_with_Scala-HandsOn_z_dużymi_danymi\\SparkScala\\customer-orders.csv")

    val rdd = lines.map(extractCustomerPricePairs)

    val totalAmountPerCustomer = rdd.reduceByKey((x, y) => x + y)

    val totalAmountPerCustomerSorted = totalAmountPerCustomer.map(x => (x._2, x._1)).sortByKey().collect()

    for (result <- totalAmountPerCustomerSorted) {
      val totalAmountSpent = result._1
      val customerId = result._2
      println(s"$customerId: $totalAmountSpent")
    }
  }

}
  