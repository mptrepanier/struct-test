import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._
import util.SchemaToCaseClass


import org.apache.spark.sql.catalyst.ScalaReflection
case class Annotation(
    field1: String,
    field2: String,
    field3: Int,
    field4: Double,
    field5: Int,
    field6: List[Mapping]
)

case class Mapping(
    fieldA: String,
    fieldB: String,
    fieldC: String,
    fieldD: String,
    fieldE: String
)

object StructTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    /*
    val annotationStruct =
      ArrayType(
        StructType(
          Array(
            StructField("field1", StringType, nullable = true),
            StructField("field2", StringType, nullable = true),
            StructField("field3", IntegerType, nullable = false),
            StructField("field4", DoubleType, nullable = false),
            StructField("field5", IntegerType, nullable = false),
            StructField(
              "field6",
              ArrayType(
                StructType(
                  Array(
                    StructField("fieldA", StringType, nullable = true),
                    StructField("fieldB", StringType, nullable = true),
                    StructField("fieldC", StringType, nullable = true),
                    StructField("fieldD", StringType, nullable = true),
                    StructField("fieldE", StringType, nullable = true)
                  )
                )
              ),
              nullable = true
            )
          )
        )
      )
      */

    val annotationSchema = ScalaReflection.schemaFor[Annotation].dataType.asInstanceOf[StructType]
    val annotation = Annotation("1", "2", 1, .5, 1, List(Mapping("a", "b", "c", "d", "e")))
    val df = List(1).toDF
    val schema    = df.schema
    val newSchema = schema.add("annotations", ArrayType(annotationSchema), false)
    val rdd       = df.rdd.map(x => Row.fromSeq(x.toSeq +: Seq(annotation)))
    val newDF = spark.createDataFrame(rdd, newSchema)
    newDF.show
  }
}
