package sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object Networksocket {
  
  def main(args: Array[String]): Unit = {
   
    //val spark = SparkSession.builder()    .appName("Network Socket Streaming")    .master("local[2]")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("Network Word Count")
    
    val ssc = new StreamingContext(conf,Seconds(1))
    
    val lines = ssc.socketTextStream("10.0.2.15", 9999)
    
    val wordcount = lines.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    
    wordcount.print()
    
    ssc.start()
    
    ssc.awaitTermination()
    
  }
}