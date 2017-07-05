import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object ScalaUDAFExample {
  // Define the SparkSQL UDAF logic
  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    // Define the UDAF input and result schema's
    def inputSchema: StructType =     // Input  = (Double price, Long quantity)
      new StructType().add("price", DoubleType).add("quantity", LongType)
    def bufferSchema: StructType =    // Output = (Double total)
      new StructType().add("total", DoubleType)
    def dataType: DataType = DoubleType
    def deterministic: Boolean = true // true: our UDAF's output given an input is deterministic
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0)           // Initialize the result to 0.0
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum   = buffer.getDouble(0) // Intermediate result to be updated
      val price = input.getDouble(0)  // First input parameter
      val qty   = input.getLong(1)    // Second input parameter
      buffer.update(0, sum + (price * qty))   // Update the intermediate result
    }
    // Merge intermediate result sums by adding them
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }
    // THe final result will be contained in 'buffer'
    def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }
  def main (args: Array[String]) {
      val spark = SparkSession
      .builder()
      .appName("Spark UDF")
      .master("local[*]")
      .getOrCreate()
    val testDF = spark.read.json("file:///G:/Hadoop/SampleData/inventory.json")
    testDF.createOrReplaceTempView("inventory") 
    // Register the UDAF with our SQLContext
    spark.udf.register("SUMPRODUCT", new SumProductAggregateFunction)
    spark.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) AS InventoryValuePerMake FROM inventory GROUP BY Make").show()
  }
}