import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col

object ParquetProcessing {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ParquetProcessing")
      .master("local")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    // Read Parquet data
    val parquetData = spark.read.parquet("C:/Users/Vishv.Patel/Desktop/Assignment-3/src/main/resources/sample.parquet")

    // Define UDFs
    val updateRouteTownCodeUDF = udf((routetowncode: String) => if (routetowncode == null) "NONE" else routetowncode)
    val updateMediaLengthUDF = udf((medialength: Double) => if (medialength == 0) 6 else medialength)

    // Apply transformations
    val processedData = parquetData
      .filter(!functions.col("modeltype").isin("static", "Static", "STATIC"))
      .withColumn("routetowncode", updateRouteTownCodeUDF(parquetData("routetowncode")))
      .withColumn("medialength", updateMediaLengthUDF(parquetData("medialength")))

    processedData.show(5);

    val cassandraHost = "127.0.0.1"
    val cassandraKeyspace = "assignment_3"
    val cassandraTable = "frame"

    val selectedColumns = Seq(
      "routeframecode", "businessareacode", "businessareagroupcode",
      "businessareaname", "channelname",
      "digital"
    )
    val selectedData = processedData.select(selectedColumns.map(col): _*)

    selectedData.write
      .format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.host", cassandraHost)
      .option("keyspace", cassandraKeyspace)
      .option("table", cassandraTable)
      .mode("append").save()

    // Write to CSV on local file system
    processedData.write
      .option("header", true)
      .csv("C:/Users/Vishv.Patel/Desktop/Assignment-3/src/main/resources/output_csv")

    spark.stop()
  }
}
