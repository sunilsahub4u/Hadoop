package sparkStreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StateFullTransformation {
  
  def updateFunc(values: Seq[Int], runningcount: Option[Int]) ={
    val newCount = values.sum + runningcount.getOrElse(0)
    new Some(newCount)
  }
  
  def main(args: Array[String]): Unit = {
  
    
    val ssc = new StreamingContext("local[2]","StateFullWordCount",Seconds(10))
    
    val lines = ssc.textFileStream("G:/Hadoop/SampleData/StreamingDataFolder")
    ssc.checkpoint("G:/Hadoop/SampleData/checkpoint")
    val wordcount = lines.flatMap(_.split(" ")).map(x=> (x,1)).reduceByKey(_+_)
    println("Word Count")
    wordcount.print()
    
    val totalwc = wordcount.updateStateByKey(updateFunc _)
    println("Total Word count")
    totalwc.print
    ssc.start()
    ssc.awaitTermination()
  }
}