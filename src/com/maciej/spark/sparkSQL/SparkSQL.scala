package com.maciej.spark.sparkSQL

import org.apache.log4j._
import org.apache.spark.sql._

object SparkSQL {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int) // these will make up columns in our dataset table.
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0 to work with Spark SQL
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/Temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // If our 'friends' data would be in some other database or json we could load that up directly and create dataset out of it right away.
    val lines = spark.sparkContext.textFile("../fakefriends.csv") // we need to first structure our data before going to dataset.
    val people = lines.map(mapper) // now we have structured RDD;
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = people.toDS // we have dataset now of Person objects;
    
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")  // register as a table;
    // Now we have a little sql database sitting in memory inside of Spark distributed potentially on a cluster.
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}