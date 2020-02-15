package org.apache.spark.sql

object DataGenerator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(classOf[App].getName)
      .master("local[*]")
      .getOrCreate()

    val df = RandomDataGenerator.randomDataFrame(spark).coalesce(1).cache()

    Map(
      "orc" -> Seq("lzo", "snappy", "zlib", "none"),
      "avro" -> Seq("deflate", "snappy", "bzip2", "xz", "uncompressed"),
      "json" -> Seq("bzip2", "deflate", "gzip", "none"),
      "parquet" -> Seq("snappy", "gzip", "none")
    )
      .foreach { case (format, compressions) =>
        compressions.foreach { compression =>
          df.write.format(format).mode("overwrite")
            .option("compression", compression)
            .save(s"output/$format/$compression")
        }
      }
  }
}
