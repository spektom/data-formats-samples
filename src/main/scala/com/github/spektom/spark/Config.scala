package com.github.spektom.spark

case class Config(outputDir: String = "output",
                  flatSchemaFields: Int = 50,
                  hierSchemaFields: Int = 200,
                  rowsNumber: Int = 100,
                  probabilityOfInteresting: Double = 0.1f,
                  probabilityOfNull: Double = 0.1f,
                  probabilityOfNestedMap: Double = 0.4f,
                  probabilityOfNestedArray: Double = 0.3f,
                  maxTextLength: Int = 1024,
                  maxPrimitiveArraySize: Int = 128,
                  maxNestedArraySize: Int = 20,
                  maxPrimitiveMapSize: Int = 128,
                  maxNestedMapSize: Int = 20)

object Config {

  def parseArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("data-formats-samples") {
      help("help").text("prints this usage text")

      opt[String]("output-dir").action((x, c) =>
        c.copy(outputDir = x)).text("Output directory")

      opt[Int]("flat-columns-number").action((x, c) =>
        c.copy(flatSchemaFields = x)).text("Columns number in flat schema")

      opt[Int]("hier-columns-number").action((x, c) =>
        c.copy(hierSchemaFields = x)).text("Columns number in hierarchical schema")

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

      opt[Int]("max-text-length").action((x, c) =>
        c.copy(maxTextLength = x)).text("Maximal length of generated text")

      opt[Int]("max-array-size").action((x, c) =>
        c.copy(maxPrimitiveArraySize = x)).text("Maximal size of generated array with primitive values")

      opt[Int]("max-nested-array-size").action((x, c) =>
        c.copy(maxNestedArraySize = x)).text("Maximal size of generated array with nested values")

      opt[Int]("max-map-size").action((x, c) =>
        c.copy(maxPrimitiveArraySize = x)).text("Maximal size of generated map with primitive values")

      opt[Int]("max-nested-map-size").action((x, c) =>
        c.copy(maxNestedArraySize = x)).text("Maximal size of generated map with nested values")
    }

    parser.parse(args, Config()) match {
      case Some(config) => config
      case None => sys.exit(1)
    }
  }
}
