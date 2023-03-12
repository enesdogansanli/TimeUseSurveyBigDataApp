# BigDataSpendTimeProject

- [BigDataSpendTimeProject](#bigdataspendtimeproject)
  - [INTRODUCTION](#introduction)
  - [ADD FUNCTIONS](#add-functions)
  - [RESULTS](#results)
  - [REFERENCES](#references)

## INTRODUCTION

Within the scope of this project, a data set that contains the daily activities of the people and the time they spend on these activities has been studied.

The project was developed in the Scala (version: 2.13.10) environment. Before running the project, the dataset path must be changed from within the code.

In addition to Scala environment apache.spark.core_2.13 and
The apache.spark.sql_2.13 libraries must be added to the project.

After the necessary environment preparations were made, first the data set was added to the Scala environment.

The structure was reconstructed by defining the variable types in the dfShcema function, which was added later. Then, a function was written that will enable us to read line by line the structure created with the row function.

Considering the beginning of the column numbers specified in the report, the structure containing the columns belonging to primary_needs, working, and other activities have been created.

With the TimeUsageSummary function, a DataFrame structure was created, including primary_needs, working, and other information, by classifying according to working (working status), age, and sex (gender) characteristics.

The DataFrame structure has been finalized with TimeUsageGrouped.

## ADD FUNCTIONS

```scala
  def dfSchema(columnNames: List[String]): StructType = {

    val fields = columnNames.map(name => name match {
      case head if (name == columnNames.head) => StructField(head, StringType, nullable = false)
      case other => StructField(other, DoubleType, nullable = false)
    })
    StructType(fields)
  }
```

```scala
  def row(line: List[String]): Row = {
    val row2 = Row.fromSeq((line.head :: line.tail.map(_.toDouble)))
    row2
  }
```

```scala
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
```

```scala
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
```

## RESULTS

## REFERENCES
https://www.kaggle.com/datasets/bls/american-time-use-survey
