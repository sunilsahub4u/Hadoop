import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


case class Record(key: Int,value: String)

object RDDRelation {
  
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                .builder()
                .appName("Spark Example")
                .config("hadoop.home.dir", "G:/Hadoop/spark-master/hadooponwindows-master/hadooponwindows-master/bin/winutils.exe")
                .master("local[*]")
                .getOrCreate()
                
    
    import spark.implicits._
    
    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    
    //df.show()
    
    df.createOrReplaceTempView("records")
    
    //println("Result of SELECT *:")
    //val sel = spark.sql("SELECT * FROM records").collect().foreach(println)
    
    //val count = spark.sql("SELECT COUNT(*) from records").collect.head.getLong(0)
    //println(s"COUNT : $count")
    
    //val rddFromSQL = spark.sql("SELECT * from records where key < 10")
    //rddFromSQL.rdd.map(r => s"Key: ${r(0)},Value: ${r(1)}").collect().foreach(println)
    
    //df.where($"key" < 10).orderBy($"value".desc).select($"key").collect().foreach(println)//select(col, cols)
    
    //df.write.mode(SaveMode.Overwrite).parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/pair.parquet")
    val parquetFile = spark.read.parquet("G:/Hadoop/spark-master/spark-master/examples/src/main/resources/output/pair.parquet")
    //parquetFile.show()
    //parquetFile.where($"key" < 10).collect().foreach(println)
    
    parquetFile.createOrReplaceTempView("parquetFile")
    
    spark.sql("SELECT * from parquetFile").collect().foreach(println)
    
    spark.stop()
  }
}