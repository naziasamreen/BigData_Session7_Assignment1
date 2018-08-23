package task7_acadgild_scala

import scala.io.Source
import java.io.{FileNotFoundException, IOException}

object ReadFile{

    def main(args: Array[String]):Unit = { 
        val filename = "D:/Dataset_7.txt"
        println("Reading File : " + filename ) 
try {
    for (line <- Source.fromFile(filename).getLines) {
        println(line)
    }
} catch {
    case e: FileNotFoundException => println("Couldn't find that file.")
    case e: IOException => println("Got an IOException!")
}
        println("Row Count")
        val NEWLINE = 10
        var newlineCount = 0L
        
       // def lineCount(f: java.io.File): Int = {
 
         var source = null
        try {
           var source = Source.fromFile(filename)
            for (line <- source.getLines) {
                newlineCount += 1
            }
         Some(newlineCount)
          println("Number of Rows of Data : " + newlineCount)
          } catch {
            case e: Exception => None
        } 

}
}