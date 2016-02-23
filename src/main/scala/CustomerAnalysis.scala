import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import java.sql.Timestamp

object SparkContextFactory {
  val sc = new SparkContext(new SparkConf().setAppName("CustomerAnalysis"))
}

object CustomerAnalysis{  
  def main(args: Array[String]){
    /*
     * + TODO: Add the implementation to check args
     */
     if(args.length > 3)
       try{
         new CustomerAnalysisJob(args(0), args(1), args(2).toInt, args(3).toInt)
       }catch{
         case ex: NumberFormatException => println("Print provide age and click count in Int format")
         case _ => println("This request cannot be processed, please contact the core development team.")
       }
     else 
       println("Please provide sufficient arguments including: srcFile, desFile, minimum age and minimum click count")
   }
}

object ColumnConvertImplicits {
     implicit class FileStringOpt(val s: String) {
      import scala.util.control.Exception.catching
      def toIntConversion = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongConversion = catching(classOf[NumberFormatException]) opt s.toLong
      def toTimestampConversion = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
    }
}

class CustomerAnalysisJob(srcFile: String, dstFile: String, minAge: Int, minClickCnt: Int ) extends java.io.Serializable {
  val customerSchema = StructType(Seq(
    StructField("ID", IntegerType, true),
    StructField("Member_Date", TimestampType, true),
    StructField("Age", IntegerType, true),
    StructField("Gender", StringType, true),
    StructField("Product", StringType, true),
    StructField("Click_Count", IntegerType, true),
    StructField("Comment", StringType, false)
  ))

  import ColumnConvertImplicits._
  def convertRow(row:String):Row = {
    val r = row.split(",")
    Row(r(0).toIntConversion,
      r(1).toTimestampConversion,
      r(2).toIntConversion,
      r(3),
      r(4),
      r(5).toIntConversion,
      r(6))
  }
   //  val customer = sc.textFile("test.csv")
  def getCustomerDataFrame() = {
      /*
       * + TODO: Implementation to check the existence of srcFile
       */
    val rowRDD = SparkContextFactory.sc.textFile(srcFile)
    val customerRDD = rowRDD.map(convertRow(_))
    val sqlContext = new SQLContext(SparkContextFactory.sc)
    val customerDataFrame =sqlContext.createDataFrame(customerRDD, customerSchema)
    customerDataFrame
  }
  
  val customerDF = getCustomerDataFrame
  
  customerDF.printSchema
  //customerDF.show
  // Show Each Product's Average Age of Viewers.
  customerDF.groupBy("Product").avg("Age").show
  /*
   * + TODO: Implementation to check the existence of srcFile
   */
  customerDF.filter(customerDF("Age") >= minAge  && customerDF("Click_Count") >= minClickCnt ).rdd.saveAsTextFile(dstFile)

}
