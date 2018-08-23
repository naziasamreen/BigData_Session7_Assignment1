package task7_acadgild_scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source

object RDD {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "RDD")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("D:/Dataset_7.txt")
    
    // Split into words separated by a space character
     val words = input.map(x => {
      val row = x.split(",").toList
       (row.apply(0).toString, row.apply(1).toString, row.apply(2).toString , row.apply(3).toInt, row.apply(4).toInt)
            })
     val readRDD = input.collect().foreach(println)

     //task 2.2
//input.saveAsTextFile("D:/output.txt")
  val count_word = input.count()
  println("count the number of rows: "+count_word)
  
  //task 2.3 
     println(" the distinct number of subjects present in the entire school")   
        input.map(x => { val row1 = x.split(",").toList
      (row1.apply(1).toString)}).distinct().foreach(println)
          
  
      /*
      val count_student= input.map(x=> { val row2 = x.split(",").toList
      (row2.apply(0).toString)}).filter(x=> x(0)=="Mathew").collect().foreach(println)
      
     // val count_student1 = count_student.filter(x => x(0)== "Mathew").collect().foreach(println)
 
      
      // println("we: " + student)
      println("Count Student : " + count_student)*/
  }
 }