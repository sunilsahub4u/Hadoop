import java.text.SimpleDateFormat
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import javax.swing.DefaultRowSorter.Row
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.functions.col


object sparkJoin {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
                .builder()
                .appName("Join")
                .master("local[*]")
                .getOrCreate()
             
    
    
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val data = List(
        List("mike", 26, true),
        List("susan", 26, false),
        List("john", 33, true)
      )
    val data2 = List(
        List("mike", "grade1", 45, "baseball", new java.sql.Date(format.parse("1957-12-10").getTime)),
        List("john", "grade2", 33, "soccer", new java.sql.Date(format.parse("1978-06-07").getTime)),
        List("john", "grade2", 32, "golf", new java.sql.Date(format.parse("1978-06-07").getTime)),
        List("mike", "grade2", 26, "basketball", new java.sql.Date(format.parse("1978-06-07").getTime)),
        List("lena", "grade2", 23, "baseball", new java.sql.Date(format.parse("1978-06-07").getTime))
      )
    
   val rdd = spark.sparkContext.parallelize(data).map(Row.fromSeq(_))
   val rdd2 = spark.sparkContext.parallelize(data2).map(Row.fromSeq(_))
   
   //rdd.collect().foreach(println)
   //rdd2.collect().foreach(println)
     
   val schema = StructType(Array(
       StructField("name", StringType,true),
       StructField("age", IntegerType,true),
       StructField("name", BooleanType,true)
       ))
       
   val schema2 = StructType(Array(
          StructField("name2", StringType, true),
        StructField("grade", StringType, true),
        StructField("howold", IntegerType, true),
        StructField("hobby", StringType, true),
        StructField("birthday", DateType, false)
        ))
   val df = spark.sqlContext.createDataFrame(rdd, schema)
   val df2 =spark.sqlContext.createDataFrame(rdd2, schema2)
   
   //df.show()
   //df2.show()
   
   //df.join(df2, "name").show()
   //df.join(df2, "name").explain()
   
   df.join(df2,col("name2") === col("name#5"),"full").show()
   
  }
}