package com.github.spektom.spark

import java.io.File
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.apache.spark.sql.{DataFrame, SparkSession}

object App {
  private val OUTPUT_DIR = "output"

  private val FORMAT_COMPRESSIONS = Map(
    "orc" -> Seq("lzo", "snappy", "zlib", "none"),
    "avro" -> Seq("deflate", "snappy", "bzip2", "xz", "uncompressed"),
    "json" -> Seq("bzip2", "deflate", "gzip", "none"),
    "parquet" -> Seq("snappy", "gzip", "none"),
    "csv" -> Seq("gzip", "bzip2", "deflate", "none"),
    "tsv" -> Seq("gzip", "bzip2", "deflate", "none"),
    "psv" -> Seq("gzip", "bzip2", "deflate", "none")
  )

  private def writeDf(df: DataFrame, schemaName: String, format: String, humanFormat: String = null, options: Map[String, String] = Map()): Unit = {
    FORMAT_COMPRESSIONS(format).foreach { compression =>
      df.write.format(format).option("compression", compression).options(options).save(
        s"$OUTPUT_DIR/$schemaName/${if (humanFormat == null) format else humanFormat}/$compression")
    }
  }

  private def writeDf(df: DataFrame, flatSchema: Boolean): Unit = {
    val cachedDf = df.coalesce(1).cache()
    val schemaName = if (flatSchema) "flat" else "hier"
    if (flatSchema) {
      writeDf(cachedDf, schemaName, "csv", "csv", Map("quote" -> "\"", "escape" -> "\"", "delimiter" -> ","))
      writeDf(cachedDf, schemaName, "csv", "tsv", Map("quote" -> "\"", "escape" -> "\"", "delimiter" -> "\t"))
      writeDf(cachedDf, schemaName, "csv", "tsve", Map("quote" -> "\"", "escape" -> "\\", "delimiter" -> "\t"))
      writeDf(cachedDf, schemaName, "csv", "psv", Map("quote" -> "\"", "escape" -> "\"", "delimiter" -> "|"))
    }
    writeDf(cachedDf, schemaName, "orc")
    writeDf(cachedDf, schemaName, "avro")
    writeDf(cachedDf, schemaName, "json")
    writeDf(cachedDf, schemaName, "parquet")
  }

  /**
   * Renames all 'part-' files to 'dataset.ext'
   */
  private def renameFiles(): Unit = {
    Files.walkFileTree(Paths.get(OUTPUT_DIR), new SimpleFileVisitor[Path] {
      override def visitFile(t: Path, basicFileAttributes: BasicFileAttributes): FileVisitResult = {
        if (!t.toFile.isDirectory) {
          val fileName = t.getFileName.toString
          if (fileName.startsWith("part-")) {
            t.toFile.renameTo(new File(t.getParent.toString, "dataset" + fileName.substring(fileName.indexOf('.'))))
          } else {
            t.toFile.delete()
          }
        }
        FileVisitResult.CONTINUE
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(classOf[App].getName)
      .master("local[*]")
      .getOrCreate()

    scala.reflect.io.Directory(new File(OUTPUT_DIR)).deleteRecursively()

    writeDf(
      RandomDataGenerator.randomDataset(spark, numFields = 50, numRows = 100, flatSchema = true), flatSchema = true)

    writeDf(
      RandomDataGenerator.randomDataset(spark, numFields = 150, numRows = 100), flatSchema = false)

    renameFiles()
  }
}
