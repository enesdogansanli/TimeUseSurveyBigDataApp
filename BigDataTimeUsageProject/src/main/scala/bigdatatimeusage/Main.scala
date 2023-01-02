package bigdatatimeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read()
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()

    // a) How much time do we spend on primary needs compared to other activities?
    finalDf.select("primaryNeeds", "other")
      .agg(functions.mean("primaryNeeds"), functions.mean("other"))
      .show()

    // b) Do women and men spend the same amount of time in working?
    finalDf.groupBy("sex")
      .agg(functions.mean("work"))
      .show()

    // c) Does the time spent on primary needs change when people get older? In other words, how much time elder people allocate to leisure compared to active people?
    finalDf.groupBy("Age")
      .agg(functions.mean("other"))
      // .filter("age == active")
      .where((finalDf("age") === "active") or (finalDf("age") === "elder"))
      .show()

    // d) How much time do employed people spend on leisure compared to unemployed people?
    finalDf.groupBy("working")
      .agg(functions.mean("other"))
      .show()
  }

  def read(): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile("D:/DATA/atussum.csv")

    val headerColumns = rdd.first().split(",").to(List)
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to(List))
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

  def dfSchema(columnNames: List[String]): StructType = {

    val fields = columnNames.map(name => name match {
      case head if (name == columnNames.head) => StructField(head, StringType, nullable = false)
      case other => StructField(other, DoubleType, nullable = false)
    })
    StructType(fields)
  }

  def row(line: List[String]): Row = {
    val row2 = Row.fromSeq((line.head :: line.tail.map(_.toDouble)))
    row2
  }

  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {

    def columnNameStartsWithKey(colName: String, keys: List[String]):Boolean = !keys.filter(key => colName.startsWith(key)).isEmpty

    val primaryNeedsKeys = List("t01", "t03", "t11", "t1801", "t1803")
    val primaryNeedsCols = columnNames.filter(col =>  columnNameStartsWithKey(col, primaryNeedsKeys))

    val workingActivitiesKeys = List("t05", "t1805")
    val workingActivitiesCols = columnNames.filter(col => columnNameStartsWithKey(col, workingActivitiesKeys))

    val otherActivitiesKeys = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
    val otherActivitiesCols = columnNames
      .filter(col => columnNameStartsWithKey(col, otherActivitiesKeys)) diff primaryNeedsCols diff workingActivitiesCols

    (primaryNeedsCols map(new Column(_)), workingActivitiesCols map (new Column(_)), otherActivitiesCols map (new Column(_)))
  }

  def timeUsageSummary(
                        primaryNeedsColumns: List[Column],
                        workColumns: List[Column],
                        otherColumns: List[Column],
                        df: DataFrame
                      ): DataFrame = {
    val workingStatusProjection: Column = when($"telfs" < 3.0 && $"telfs" >= 1.0, "employed").otherwise("unemployed").as("working")
    val sexProjection: Column = when($"tesex" === 1.0, "male").otherwise("female").as("sex")
    val ageProjection: Column = when($"teage" >= 15.0  && $"teage" <= 22.0, "young")
      .when($"teage" >= 23.0  && $"teage" <= 55.0, "active")
      .otherwise("elder")
      .as("age")

    val primaryNeedsProjection: Column = (primaryNeedsColumns.reduce(_+_)/60.0).as("primaryNeeds")
    val workProjection: Column = (workColumns.reduce(_+_)/60.0).as("work")
    val otherProjection: Column = (otherColumns.reduce(_+_)/60.0).as("other")
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force
  }

  def timeUsageGrouped(summed: DataFrame): DataFrame =
    summed.groupBy($"working", $"sex",$"age")
      .agg(round(avg("primaryNeeds"),1).as("primaryNeeds"),
        round(avg("work"),1).as("work"),
        round(avg("other"),1).as("other"))
      .sort($"working", $"sex", $"age")
}

case class TimeUsageRow(
                         working: String,
                         sex: String,
                         age: String,
                         primaryNeeds: Double,
                         work: Double,
                         other: Double
                       )