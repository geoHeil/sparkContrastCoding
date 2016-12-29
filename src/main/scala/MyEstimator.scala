package org.apache.spark.ml.feature

// deliberately in other package to access spark private methods

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model, _}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, JObject, NoTypeHints, native, _}

/**
  * performs biased contrast coding of X in relation to y per a group
  *
  * Invariant: columnsToCode and columnsToDrop are disjunct
  */
trait PercentageCoderParams {
  var columnsToCode: Seq[String] = Seq() //column names which need to be coded
  var columnsToDrop: Seq[String] = Seq() //column names contain too many levels and need to be dropped
  var target: String = "TARGET" // column name of target column
  /**
    * Performs 2 types of contrast coding for both (columnsToDrop and columnsToCode),
    * additionally keeps the raw columns for all columns specified in columnsToCode
    */
  var mode: String = "bothAdditionally" //mode how to perform the coding
}

class PercentageCoder(override val uid: String) extends Estimator[PercentageCoderModel]
  with DefaultParamsWritable with Logging
  with PercentageCoderParams {
  def this() = this(Identifiable.randomUID("percentageTransformer"))

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)

  def copy(extra: ParamMap): PercentageCoder = defaultCopy(extra)

  def setColsToTransform(value: Seq[String]): this.type = {
    this.columnsToCode = value
    this
  }

  def setColsToDrop(value: Seq[String]): this.type = {
    this.columnsToDrop = value
    this
  }

  def setTarget(value: String): this.type = {
    this.target = value
    this
  }

  def setMode(value: String): this.type = {
    this.mode = value
    this
  }

  override def transformSchema(schema: StructType): StructType = {
    logger.debug("original " + schema.toString)
    var transformedSchema = schema
    for (colName <- columnsToCode ++ columnsToDrop) {
      val idx = schema.fieldIndex(colName)
      val field = schema.fields(idx)
      if (field.dataType != StringType) {
        throw new Exception(s"Input type  ${field.dataType} did not match input type StringType")
      }

      mode match {
        case "bothAdditionally" => {
          if (!this.columnsToDrop.contains(colName)) {
            transformedSchema.add(StructField("pre_" + colName, DoubleType, false))
            transformedSchema.add(StructField("pre2_" + colName, DoubleType, false))
          } else {
            transformedSchema = StructType(transformedSchema.fields.filter(_.name != colName))
            transformedSchema.add(StructField("pre_" + colName, DoubleType, false))
            transformedSchema.add(StructField("pre2_" + colName, DoubleType, false))
          }
        }
        case _ => throw new NotImplementedError()
      }
    }
    logger.debug("created schema " + transformedSchema.toString)
    transformedSchema
  }

  override def fit(df: Dataset[_]): PercentageCoderModel = {
    val targetCounts = df.filter(df(target) === 1).groupBy(target).agg(count(target).as("cnt_foo_eq_1"))
    val newDF = df.toDF.join(broadcast(targetCounts), Seq(target), "left")
    newDf.cache
    val res = (columnsToDrop ++ columnsToCode).toSet.foldLeft(newDF) {
      (currentDF, colName) =>
      {
        logger.info("using col " + colName)
        handleBias(currentDF, colName) // TODO optimize this function + waiting time before it is called
      }
    }
      .drop("cnt_foo_eq_1")

    val combined = ((columnsToDrop ++ columnsToCode).toSet).foldLeft(res) {
      (currentDF, colName) =>
      {
        currentDF
          .withColumn("combined_" + colName, map(col(colName), array(col("pre_" + colName), col("pre2_" + colName))))
      }
    }

    val columnsToUse = combined
      .select(combined.columns
        .filter(_.startsWith("combined_"))
        map (combined(_)): _*)

    val newNames = columnsToUse.columns.map(_.split("combined_").last)
    val renamed = columnsToUse.toDF(newNames: _*)

    val cols = renamed.columns
    val localData = renamed.collect // TODO collect asynchronously & get rid of collect

    val columnsMap = cols.map { colName =>
      colName -> localData.flatMap(_.getAs[Map[String, Seq[Double]]](colName)).toMap
    }.toMap

    copyValues(new PercentageCoderModel(
      uid
    ).setColsToDrop(this.columnsToDrop)
      .setColsToTransform(this.columnsToCode)
      .setTarget(this.target)
      .setMode(this.mode)).setParent(this)
      .setFittedResults(columnsMap)
  }

  private def handleBias(df: DataFrame, colName: String, target: String = this.target) = {
    val w1 = Window.partitionBy(colName)
    val w2 = Window.partitionBy(colName, target)

    df.withColumn("cnt_group", count("*").over(w2))
      .withColumn("pre2_" + colName, mean(target).over(w1))
      .withColumn("pre_" + colName, coalesce(min(col("cnt_group") / col("cnt_foo_eq_1")).over(w1), lit(0D)))
      .drop("cnt_group")
  }
}

class PercentageCoderModel private[feature] (
                                              override val uid: String
                                            )
  extends Model[PercentageCoderModel] with MLWritable with PercentageCoderParams {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)

  var results: Map[String, Map[String, Seq[Double]]] = Map()

  def setColsToTransform(value: Seq[String]): this.type = {
    this.columnsToCode = value
    this
  }

  def setFittedResults(value: Map[String, Map[String, Seq[Double]]]): this.type = {
    this.results = value
    this
  }

  def setColsToDrop(value: Seq[String]): this.type = {
    this.columnsToDrop = value
    this
  }

  def setTarget(value: String): this.type = {
    this.target = value
    this
  }

  def setMode(value: String): this.type = {
    this.mode = value
    this
  }

  val joinUDF = udf((newColumn: String, newValue: String, codingVariant: Int) => {
    results.get(newColumn) match {
      case Some(tt) => {
        val nestedArray = tt.getOrElse(newValue, Seq(0.0))
        if (codingVariant == 0) {
          nestedArray.head
        } else {
          nestedArray.last
        }
      }
      case None => throw new Exception("Column not contained in initial data frame")
    }
  })

  override def transform(df: Dataset[_]): DataFrame = {
    transformSchema(df.schema, logging = true)

    (results).foldLeft(df.toDF) {
      (currentDF, colName) =>
      {
        currentDF
          .withColumn("pre_" + colName._1, joinUDF(lit(colName._1), col(colName._1), lit(0)))
          .withColumn("pre2_" + colName._1, joinUDF(lit(colName._1), col(colName._1), lit(1)))
      }
    }.drop(this.columnsToDrop: _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    var transformedSchema = schema
    for (colName <- columnsToCode ++ columnsToDrop) {
      val idx = schema.fieldIndex(colName)
      val field = schema.fields(idx)
      if (field.dataType != StringType) {
        throw new Exception(s"Input type  ${field.dataType} did not match input type StringType")
      }

      mode match {
        case "bothAdditionally" => {
          if (!this.columnsToDrop.contains(colName)) {
            transformedSchema.add(StructField("pre_" + colName, DoubleType, false))
            transformedSchema.add(StructField("pre2_" + colName, DoubleType, false))
          } else {
            transformedSchema = StructType(transformedSchema.fields.filter(_.name != colName))
            transformedSchema.add(StructField("pre_" + colName, DoubleType, false))
            transformedSchema.add(StructField("pre2_" + colName, DoubleType, false))
          }
        }
        case _ => throw new NotImplementedError()
      }
    }
    transformedSchema
  }

  override def copy(extra: ParamMap): PercentageCoderModel = defaultCopy(extra)

  override def write: MLWriter = new PercentageCoderModel.PercentageTransformerModelWriter(this)

}

object PercentageCoderModel extends MLReadable[PercentageCoderModel] {

  override def read: MLReader[PercentageCoderModel] = new PercentageTransformerModelReader

  override def load(path: String): PercentageCoderModel = super.load(path)

  private[PercentageCoderModel] class PercentageTransformerModelWriter(instance: PercentageCoderModel)
    extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      implicit val formats = native.Serialization.formats(NoTypeHints)
      val extraMetadata: JObject = JObject(
        "columnsToCode" -> instance.columnsToCode,
        "columnsToDrop" -> instance.columnsToDrop,
        "target" -> instance.target,
        "mode" -> instance.mode,
        "result" -> instance.results
      )
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
    }
  }

  private class PercentageTransformerModelReader
    extends MLReader[PercentageCoderModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[PercentageCoderModel].getName

    override def load(path: String): PercentageCoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      implicit val format = DefaultFormats

      val model = new PercentageCoderModel(metadata.uid)
        .setColsToDrop((metadata.metadata \ "columnsToDrop").extract[Seq[String]])
        .setColsToTransform((metadata.metadata \ "columnsToCode").extract[Seq[String]])
        .setTarget((metadata.metadata \ "target").extract[String])
        .setMode((metadata.metadata \ "mode").extract[String])
        .setFittedResults((metadata.metadata \ "result").extract[Map[String, Map[String, Seq[Double]]]])

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

}
