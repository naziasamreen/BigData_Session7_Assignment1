package task7_acadgild_scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.io.{FileNotFoundException, IOException}

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("D:/Dataset_7.txt")
    
    // Split into words separated by a space character
    
  val words = input.flatMap(x => x.split(","))
   
    // Count up the occurrences of each word
   val wordCounts = words.countByValue()
   val wc = words.count()
   // Print the results.
    wordCounts.foreach(println)
    println("no. of words " + wc)
    
    //Task 1.3 Counting words with separator as '-'
    val words1 = words.flatMap(x=> x.split("-"))
    
       // Count up the occurrences of each word
   val wordCounts1 = words1.countByValue()
   val wc1 = words1.count()
   // Print the results.
    wordCounts1.foreach(println)
    println("no. of words with separator '-' " + wc1)
  }
  
}
