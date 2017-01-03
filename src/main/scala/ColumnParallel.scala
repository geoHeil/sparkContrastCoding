import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class MyClass(TARGET: Int, col1:String, col2:String, col3TooMany:String, col4:String)

object ColumnParallel extends App {

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

  Logger.getLogger("org").setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(this.getClass)

  val conf: SparkConf = new SparkConf()
    .setAppName("columnParallel")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val inputDf = input.toDF("TARGET", "col1", "col2", "col3TooMany", "col4").as[MyClass]
//  val inputDs = inputDf.as[MyClass]

  // TODO try out parallelization with spark-RDD API. From http://stackoverflow.com/questions/25171070/spark-processing-columns-in-parallel
  // so far: not really sure how to use it for contrast coding
  val rdd = spark.sparkContext.parallelize((1 to 4).map(x => Seq("x_0", "x_1", "x_2", "x_3")))
  rdd.toDF.show
  //  rdd: org.apache.spark.rdd.RDD[Seq[String]] = ParallelCollectionRDD[26] at parallelize at <console>:12

  // original TODO what is actually better ? original or intellij's suggestions?
  //  val rdd1 = rdd.flatMap { x => {(0 to x.size - 1).map(idx => (idx, x(idx)))}}
  // suggestions by intellij
  //  val rdd1 = rdd.flatMap { x => {(0 until x.size).map(idx => (idx, x(idx)))}}
  val rdd1 = rdd.flatMap { x => {x.indices.map(idx => (idx, x(idx)))}}
  rdd1.toDF.show
  inputDf.rdd.toDF.show
//  inputDf.rdd.keyBy(_.TARGET).groupByKey.foreach{println}
  // TODO no access to index
//  val rdd1_inputDf = inputDf.rdd.flatMap { x => {x.indices.map(idx => (idx, x(idx)))}}
//  rdd1_inputDf.toDF.show
  //  rdd1: org.apache.spark.rdd.RDD[(Int, String)] = FlatMappedRDD[27] at flatMap at <console>:14

  val rdd2 = rdd1.map(x => (x, 1))
  rdd2.toDF.show
  //    rdd2: org.apache.spark.rdd.RDD[((Int, String), Int)] = MappedRDD[28] at map at <console>:16

  val rdd3 = rdd2.reduceByKey(_ + _)
  rdd3.toDF.show

  // TODO use aggregate by key
  // rdd2.aggregateByKey(0)((k,v) => v.toInt+k, (v,k) => k+v).toDF.show

  //      rdd3: org.apache.spark.rdd.RDD[((Int, String), Int)] = ShuffledRDD[29] at reduceByKey at <console>:18

  rdd3.take(4)
  //        res22: Array[((Int, String), Int)] = Array(((0,x_0),4), ((3,x_3),4), ((2,x_2),4), ((1,x_1),4))


  spark.stop
}
