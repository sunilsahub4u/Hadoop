import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object SQLDataSourceExample {
  def main(args: Array[String]): Unit = {
    
    
    val spark = SparkSession
    .builder()
    .appName("spark example")
    .config("acv","ad")
    .master("local[*]")
    .getOrCreate()
    
    //runBasicDataSourceExample(spark)
    //runBasicParquetExample(spark)
/*    runParquetSchemaMergingExample(spark)*/
    runJsonDatasetExample(spark)
    //Stop Spark
    spark.stop()
  }
  
  private def runBasicDataSourceExample(spark : SparkSession) : Unit ={
    
    val userDF = spark.read.load("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/users.parquet")
    //userDF.show()
    
    val userjson = userDF.select("name", "favorite_color").write
    .mode(SaveMode.Overwrite)
    .save("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/namesAndFavColors.json")
//    val userjsonread = spark.read
//    .load("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/namesAndFavColors.json")
//    userjsonread.show()
    
    val sqlDF = spark.sql("SELECT * FROM pp.`G:/Hadoop/spark-master/spark-master/examples/src/main/resources/users.parquet`")
    
    sqlDF.show()
  }
  
  private def runBasicParquetExample(spark:SparkSession) : Unit ={
    
    import spark.implicits._
    
    val peopleDF= spark.read.json("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/people.json")
    
    peopleDF.write.parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/people.parquet")
    
    val peopleFileDF = spark.read.parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/people.parquet")
    
    peopleFileDF.createOrReplaceTempView("pparquet")
    
    val namesDF = spark.sql("SELECT * FROM pparquet")
    
    namesDF.map(x => "Name" + x(0)).show()
  }
  
  
  private def runParquetSchemaMergingExample(spark : SparkSession) : Unit ={
    
    import spark.implicits._
    
    val squresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i,i*i)).toDF("value","square")
    squresDF.write.mode(SaveMode.Overwrite).parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/test_table/key=1")
    
    
    val cubeDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i,i*i*i,444)).toDF("value","cube","fixed")
    cubeDF.write.mode(SaveMode.Overwrite).parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/test_table/key=2")
    
    val mergedDF = spark.read.option("mergeSchema", "true")
     .parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/test_table")
    
     mergedDF.printSchema()
    mergedDF.collect().foreach(println)  
  }
  
  private def runJsonDatasetExample(spark : SparkSession): Unit={
    
    import spark.implicits._
    
    /*val path = "G:/Hadoop/spark-master/spark-master/examples/src/main/resources"
    
    val peopleDF = spark.read.json(path+"/people.json")
    peopleDF.show()
   
    peopleDF.printSchema()
    
    peopleDF.createOrReplaceTempView("people")
    
    spark.sql("SELECT * FROM people WHERE age BETWEEN 13 AND 20").show()*/
    
    val otherpeopleDS = spark.sparkContext.makeRDD("""{"name":"Sunil","address":{"city":"Bhopal","State":"MP"}}""" :: Nil)
    
//    otherpeopleDS.printSchema()
    val otherpeople = spark.read.json(otherpeopleDS)
    otherpeople.show()
    
    
  }
}