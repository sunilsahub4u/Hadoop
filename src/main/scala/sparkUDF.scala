import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object sparkUDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark UDF")
      .master("local[*]")
      .getOrCreate()

    /*    
    val df = Seq((0, "hello"),(1, "world")).toSeq
    case class Person(name: String, age: Int)

val personDS = Seq(("Max", 33), ("Adam", 32), ("Muller", 62)).map(x => (x._1,x._2))
import spark.implicits._

val df1 = personDS.toDF("text","age")


    val upperUDF = udf {s : String => s.toUpperCase()}
    //df1.withColumn("upper", upperUDF('text)).show

    spark.udf.register("myUpper", (s : String) => s.toUpperCase())
    
    spark.catalog.listFunctions.filter('name like "%pper%").show()
  */

    ///Type 2

    val dftemp = spark.read.json("file:///G:/Hadoop/SampleData/temperatures.json")

    import spark.implicits._
    dftemp.createOrReplaceTempView("temerature")

    spark.sql("SELECT * FROM temerature").show()
    spark.udf.register("CTOF", (degreesCelsius: Integer) => (degreesCelsius * 9.0 / 5.0) + 32.0 )
    
    spark.sql("SELECT city, CTOF(avgHigh), CTOF(avgLow) FROM temerature").show()

  }
}