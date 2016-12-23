package at.ac.tuwien.thesis.heiler.ml

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.scalatest.FunSuite

/**
 * This coder assumes that all null values were imputed before using it!
 */
case class ExpectedOutput(TARGET: Int, col1: String, col2: String, col4: String,
  pre_col1: Option[Double], pre_col2: Option[Double],
  pre2_col1: Option[Double], pre2_col2: Option[Double],
  pre_col3TooMany: Option[Double], pre2_col3TooMany: Option[Double])

class PercentageCoderTest extends FunSuite with SharedSparkContext with DatasetSuiteBase {

  val input = Seq(
    (0, "A", "B", "C", "D"),
    (1, "A", "B", "C", "D"),
    (0, "d", "a", "jkl", "d"),
    (0, "d", "g", "C", "D"),
    (1, "A", "d", "t", "k"),
    (1, "d", "c", "C", "D"),
    (1, "c", "B", "C", "D")
  )
  val inputToDrop = Seq("col3TooMany")
  val inputToBias = Seq("col1", "col2")

  val expectedOutput = Seq(
    ExpectedOutput(0, "A", "B", "D", Some(0.50), Some(0.50), Some(0.666667), Some(0.666667), Some(0.75), Some(0.60)),
    ExpectedOutput(1, "A", "B", "D", Some(0.50), Some(0.50), Some(0.666667), Some(0.666667), Some(0.75), Some(0.60)),
    ExpectedOutput(0, "d", "a", "d", Some(0.25), Some(0.00), Some(0.333333), Some(0.000000), Some(0.00), Some(0.00)),
    ExpectedOutput(0, "d", "g", "D", Some(0.25), Some(0.00), Some(0.333333), Some(0.000000), Some(0.75), Some(0.60)),
    ExpectedOutput(1, "A", "d", "k", Some(0.50), Some(0.25), Some(0.666667), Some(1.000000), Some(0.25), Some(1.00)),
    ExpectedOutput(1, "d", "c", "D", Some(0.25), Some(0.25), Some(0.333333), Some(1.000000), Some(0.75), Some(0.60)),
    ExpectedOutput(1, "c", "B", "D", Some(0.25), Some(0.50), Some(1.000000), Some(0.666667), Some(0.75), Some(0.60))
  )

  test("Percentage transformer with mode bothAdditionally should transform") {
    val session = spark
    import session.implicits._
    val inputDf = input.toDF("TARGET", "col1", "col2", "col3TooMany", "col4").withColumn("rowId", monotonically_increasing_id())
    val expectedOutputDf = expectedOutput.toDF

    val pct = new PercentageCoder()
      .setColsToDrop(inputToDrop)
      .setColsToTransform(inputToBias)
      .setTarget("TARGET")

    time { pct.fit(inputDf) }
    val pctModel = pct.fit(inputDf)

    time { pctModel.transform(inputDf) }
    val result = pctModel.transform(inputDf)

    val orderedResult = result
      .select("TARGET", "col1", "col2", "col4", "pre_col1", "pre_col2", "pre2_col1", "pre2_col2", "pre_col3TooMany", "pre2_col3TooMany")
      .orderBy('rowId)

    assertDataFrameApproximateEquals(expectedOutputDf, orderedResult, 0.001)
  }

  test("Percentage transformer should persist correctly") {
    val session = spark
    import session.implicits._
    val inputDf = input.toDF("TARGET", "col1", "col2", "col3TooMany", "col4").withColumn("rowId", monotonically_increasing_id())
    val expectedOutputDf = expectedOutput.toDF

    val result = new PercentageCoder()
      .setColsToDrop(inputToDrop)
      .setColsToTransform(inputToBias)
      .setTarget("TARGET")
      .fit(inputDf)
    result.write.overwrite.save("testingPersistencePer")
    val loaded = PercentageCoderModel.load("testingPersistencePer")

    val loadedResult = loaded.transform(inputDf)
    val orderedResult = loadedResult
      .select("TARGET", "col1", "col2", "col4", "pre_col1", "pre_col2", "pre2_col1", "pre2_col2", "pre_col3TooMany", "pre2_col3TooMany")
      .orderBy('rowId)

    assertDataFrameApproximateEquals(expectedOutputDf, orderedResult, 0.001)
  }

  test("Unknown value should be set to 0") {
    val session = spark
    import session.implicits._
    val inputDf = input.toDF("TARGET", "col1", "col2", "col3TooMany", "col4")
    val inputNewDf = Seq(("NewValue", "NewValue", "C", "D")).toDF("col1", "col2", "col3TooMany", "col4")

    val expectedOutputNew = Seq(("NewValue", "NewValue", "D", Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.75), Some(0.60)))
    val expectedOutputDf = expectedOutputNew.toDF("col1", "col2", "col4", "pre_col1", "pre_col2",
      "pre2_col1", "pre2_col2", "pre_col3TooMany", "pre2_col3TooMany")

    val fittedModel = new PercentageCoder()
      .setColsToDrop(inputToDrop)
      .setColsToTransform(inputToBias)
      .setTarget("TARGET")
      .fit(inputDf)

    val result = fittedModel.transform(inputNewDf)
    val orderedResult = result
      .select("col1", "col2", "col4", "pre_col1", "pre_col2", "pre2_col1", "pre2_col2", "pre_col3TooMany", "pre2_col3TooMany")

    assertDataFrameApproximateEquals(expectedOutputDf, orderedResult, 0.001)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
}
