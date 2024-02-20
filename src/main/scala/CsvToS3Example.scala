import org.apache.spark.sql.{SparkSession, DataFrame}

object CsvToS3Example {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("CsvToS3Example")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()

    // Read CSV file with a header
    val csvData: DataFrame = spark.read.format("csv")
      .option("header", true)
      .load("src/main/resources/username.csv")

    // Show the data
    csvData.show(5)

    // Define S3 bucket and path
    val s3Bucket = "s3a://assignment-1-vishv"
    val s3Path = "username"

    // Write data to S3
    csvData.write
      .mode("overwrite")
      .csv(s"$s3Bucket/$s3Path")

    // Stop the Spark session
    spark.stop()
  }
}
