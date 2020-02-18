package com.github.spektom.spark

case class Config(outputDir: String = "output",
                  flatSchemaFields: Int = 50,
                  hierSchemaFields: Int = 200,
                  rowsNumber: Int = 100,
                  probabilityOfInteresting: Double = 0.1f,
                  probabilityOfNull: Double = 0.1f,
                  probabilityOfNestedMap: Double = 0.4f,
                  probabilityOfNestedArray: Double = 0.3f)

object Config {

  def parseArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("data-formats-samples") {
      opt[String]("output-dir").action((x, c) =>
        c.copy(outputDir = x)).text("Output directory")

      opt[Int]("flat-schema-fields").action((x, c) =>
        c.copy(flatSchemaFields = x)).text("Flat schema columns number")

      opt[Int]("hier-schema-fields").action((x, c) =>
        c.copy(hierSchemaFields = x)).text("Hierarchical schema columns number")

      opt[Int]("rows-number").action((x, c) =>
        c.copy(rowsNumber = x)).text("Rows number to generate")

      opt[Double]("prob-interesting").action((x, c) =>
        c.copy(probabilityOfInteresting = x)).text("Probability of interesting value (Inf, NaN, random text, etc.)")

      opt[Double]("prob-null").action((x, c) =>
        c.copy(probabilityOfNull = x)).text("Probability of null value")

      opt[Double]("prob-nested-map").action((x, c) =>
        c.copy(probabilityOfNestedMap = x)).text("Probability of a map containing nested values")

      opt[Double]("prob-nested-array").action((x, c) =>
        c.copy(probabilityOfNestedArray = x)).text("Probability of an array containing nested values")
    }

    parser.parse(args, Config()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException("Wrong arguments")
    }
  }
}
