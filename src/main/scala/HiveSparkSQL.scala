import org.apache.spark.sql.SparkSession


case class Records(key: Int ,value: String )
object HiveSparkSQL {
 
   def main(args: Array[String]): Unit = {
  val spark = SparkSession
  .builder()
  .appName("Spar Hive Example")
  .config("spark.sql.warehouse.dir","spark-warehouse")
  .master("local[*]")
  .enableHiveSupport()
  .getOrCreate()
  
  import spark.implicits._
  import spark.sql
  
  sql("CREATE TABLE IF NOT EXISTS src (key INT, value String)")
  sql("LOAD DATA LOCAL INPATH 'G:/Hadoop/spark-master/spark-master/examples/src/main/resources/kv1.txt' INTO TABLE src")
  
  //G:/Hadoop/spark-master/spark-master/examples/src/main/resources/kv1.txt
  
  sql("SELECT * FROM src").show() 
   }
}