import org.apache.spark.sql.SparkSession

/**
 * Spark job to check if parquet files in S3 are equivalent.
 */
object ParquetDiffer {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ParquetDiffer <folder1Path> <folder2Path>")
      System.exit(1)
    }

    val Array(folder1Path, folder2Path) = args

    val spark = SparkSession.builder()
      .appName("ParquetDiffer")
      .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
      .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")
      .getOrCreate()

    // Read all Parquet files from S3 folders
    val df1 = spark.read.parquet(folder1Path)
    val df2 = spark.read.parquet(folder2Path)

    // Compare schemas of both dataframes
    val schemaComparison = df1.schema.equals(df2.schema)
    if (schemaComparison) {
      println("The schemas of all Parquet files in both folders are identical.")
      // Perform specific comparisons based on your needs
      val contentComparison = df1.except(df2).isEmpty && df2.except(df1).isEmpty
      if (contentComparison) {
        println("The content of all Parquet files in both folders is identical.")
      } else {
        println("The content of Parquet files in both folders is different.")
      }
    } else {
      println("The schemas of Parquet files in both folders are different.")
    }

    spark.stop()
  }
}
