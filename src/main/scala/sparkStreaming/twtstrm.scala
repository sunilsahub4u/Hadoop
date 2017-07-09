package sparkStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf


object twtstrm {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterPopularTags <master>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    //val (master, filters) = (args.head, args.tail)

    args.foreach(println)
    
    val filters = args.takeRight(args.length - 4)
    
    
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
     // System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
  
}