import org.apache.spark.sql.SparkSession
import scala.tools.nsc.doc.model.Def
import org.apache.spark.annotation.Private
import org.apache.spark.sql.Row
import java.sql.Struct
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

object SparkSQLExample {

  
 case class Person(name: String, age: Long)
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .appName("Spark SQL Exp")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    
    //runBasicDataFrameExample(spark)
    runInferSchemaExample(spark)
    
  }
  
  private def runBasicDataFrameExample(spark : SparkSession): Unit = {
    val df = spark.read.json("G:/Hadoop/SampleData/people.json")
   // df.show  
    //df.printSchema()
  /* 
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
	*/
   //df.select("name").show()
   //df.select($"name",$"age" + 1).show()
   //df.filter($"age" > 21).show()
   //df.groupBy("age").count().show()
   //df.createOrReplaceTempView("people")
  // val sqlDF = spark.sql("SELECT * FROM people")
  // sqlDF.show()
   
    df.createGlobalTempView("globalPeople")
    spark.sql("SELECT * FROM global_temp.globalPeople").show()
    
    spark.newSession().sql("SELECT * FROM global_temp.globalPeople").show()
  }
  
  private def runDatasetCreationExample(spark : SparkSession): Unit={
    import spark.implicits._
    
    val caseClassDS = Seq(Person("Andy",32)).toDS()
    //caseClassDS.show()
    
    
    val premitiveDS = Seq(1,2,3).toDS()
    //premitiveDS.map(_ + 1).collect().foreach(println)
    
    
    val df = spark.read.json("G:/Hadoop/SampleData/people.json").as[Person]
    df.show()
  }
  
  private def runInferSchemaExample(spark : SparkSession): Unit={
    
    import spark.implicits._
    
    val peopleDF = spark.sparkContext
                    .textFile("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/people.txt")
                    .map(_.split(","))
                    .map( x => Person(x(0),x(1).trim.toInt))
                    .toDF
                    
   peopleDF.createOrReplaceTempView("people")
   
   spark.sql("SELECT * FROM people").show()
   
   //spark.sql("SELECT * FROM people").map(x => "Name: " + x(0) ).show()
   
   spark.sql("SELECT * FROM people").map(y => "Name: " + y.getAs[String]("name")).show()
   
   
  }
  
  private def runProgrammaticSchemaExample(spark : SparkSession): Unit={
    
    import spark.implicits._
    
    val peopleRDD = spark.sparkContext.textFile("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/people.txt")
    val rowRDD = peopleRDD.map(_.split(","))
                 .map(x => Row(x(0),x(1)))
    
    val schemastring = "name age"
    val fields = schemastring.split(" ").map(x => StructField(x,StringType,nullable=true))
    val schema = StructType(fields)
    
    
    //apply schema to RDD
    
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    
     peopleDF.createOrReplaceTempView("people")
     
     val result = spark.sql("SELECT * FORM people")
    
     result.show()
     
     result.map(x => "Name: " + x(4)).show()
     
  }
}