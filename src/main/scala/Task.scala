import java.util.Properties

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

case class resultSchema(Median: Float,ExactMedian: Float, Mode: String)

object Task extends App{
  //SparkSession
  val spark = SparkSession.builder()
    .appName("Number Theory Task")
    .master("local[3]")
    .getOrCreate()

  //CSV file schema
  val csvSchemaStruct = StructType(List(
    StructField("id", IntegerType),
    StructField("first_name", StringType),
    StructField("last_name", StringType),
    StructField("gender", StringType),
    StructField("points", IntegerType)
  ))

  //Read CSV from HDFS
  val df:DataFrame = spark.read
    .format("csv")
    .option("header","true")
    .option("path","hdfs://localhost:9820/sample/mock_data.csv")
    .schema(csvSchemaStruct)
    .load()

  // Median using Approx-Quantile algorithm, returns approximate quantile at given probability.
  val percentileArray = df.stat.approxQuantile("points",Array(0.5),0.0001)

  // Exact median(Expensive operation)
  val sortedRDD = df.sort("points").rdd.zipWithIndex().map{
    case(value, index) => (index,value)
  }
  val count = sortedRDD.count()
  val median: Float = if(count % 2 == 0){
    val l = count / 2 - 1
    val r = l + 1
    (sortedRDD.lookup(l).head.getInt(4) + sortedRDD.lookup(r).head.getInt(4)).toFloat / 2
  }else sortedRDD.lookup(count/2).head.getInt(4)

  // Mode using UDAF
  val modeFunc = new ModeUDAF
  val mode = df.agg(modeFunc(col("gender")).as("Mode")).head().getString(0)

  //Writing to MySQL database
  val url = "jdbc:mysql://localhost:3306/number_theory"
  val table = "sample"
  val properties = new Properties()
  properties.put("user","root")
  properties.put("password","sql@01")
  Class.forName("com.mysql.cj.jdbc.Driver").newInstance()

  import spark.sqlContext.implicits._
  val resultDf = Seq(resultSchema(percentileArray.head.toFloat,median,mode)).toDF()
  resultDf.write.mode(SaveMode.Overwrite).jdbc(url,table,properties)

}
