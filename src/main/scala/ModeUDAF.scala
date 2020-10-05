import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, LongType, StringType, StructField, StructType}

class ModeUDAF extends UserDefinedAggregateFunction {

  //override def inputSchema: StructType =
  //StructType(StructField("value", StringType) :: Nil)

  override def inputSchema: StructType = new StructType().add("input", StringType)

  override def bufferSchema: StructType = new StructType().add("frequencyMap", DataTypes.createMapType(StringType, LongType))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputWord = input.getAs[String](0)
    val existingMap = buffer.getAs[Map[String, Long]](0)
    buffer(0) = existingMap + (if (existingMap.contains(inputWord)) inputWord -> (existingMap(inputWord) + 1) else inputWord -> 1L)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getAs[Map[String, Long]](0)
    val map2 = buffer2.getAs[Map[String, Long]](0)
    buffer1(0) =  map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0L)) }
  }

  override def evaluate(buffer: Row): String = {
    buffer.getAs[Map[String, Long]](0).maxBy{case (key, value) => value}._1
  }
}
