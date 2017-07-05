import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Explode
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col

object NestedJson {
   def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .appName("Spark SQL Exp")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
     import spark.implicits._
    val df = spark.read.json("G:/Hadoop/SampleData/world_bank.json")
    
   // df.show()
  //majorsector_percent
    
    val dfDates = df.select(explode($"_id"),$"majorsector_percent")
    dfDates.printSchema()
    dfDates.show()
    //df.explode($"majorsector_percent"){}.cache
    
   }
}