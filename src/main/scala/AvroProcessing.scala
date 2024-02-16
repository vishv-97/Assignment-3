import org.apache.spark.sql.{SaveMode, SparkSession}

object AvroProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("AvroProcessing")
      .master("local")
      .getOrCreate()

    // Read Avro file as DataFrame
    val avroDF = spark.read.format("avro").load("src/main/resources/sample_2.avro")

    avroDF.show(5)

    // Perform filter on campaign_ref is null or empty
    val filteredDF = avroDF.filter(avroDF("pop.campaign_ref").isNotNull && avroDF("pop.campaign_ref") =!= "")

    // Prepare lookup DataFrame
    val lookupData = Seq(
      ("JCDECAUX_FR", true),
      ("JCDECAUX_FR", false),
      ("JCDECAUX_GB", false)
    )
    val lookupDF = spark.createDataFrame(lookupData).toDF("market", "isComplianceSupportedMarket")

    // Join and filter down the records
    val finalDF = filteredDF.join(lookupDF, filteredDF("header.market") === lookupDF("market"))
      .filter("isComplianceSupportedMarket")


    // Write the final DataFrame into file system in market/player_type partitions
    // Write the final DataFrame into file system partitioned by market and player_type
    finalDF
      .write
      .partitionBy("market")
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/output_parquet")

    spark.stop()
  }
}