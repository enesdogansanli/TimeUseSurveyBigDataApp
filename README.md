# BigDataSpendTimeProject

- [BigDataSpendTimeProject](#bigdataspendtimeproject)
  - [INTRODUCTION](#introduction)
  - [ADD FUNCTIONS](#add-functions)
  - [RESULTS](#results)
    - [ON SCALA](#on-scala)
    - [ON CASSANDRA](#on-cassandra)
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

### ON SCALA

* Last DataFrame Result


```
+----------+------+------+------------+----+-----+
|   working|   sex|   age|primaryNeeds|work|other|
+----------+------+------+------------+----+-----+
|  employed|female|active|        11.5| 4.2|  8.1|
|  employed|female| elder|        10.6| 3.9|  9.3|
|  employed|female| young|        11.6| 3.3|  8.9|
|  employed|  male|active|        10.8| 5.2|  7.8|
|  employed|  male| elder|        10.4| 4.8|  8.6|
|  employed|  male| young|        10.9| 3.7|  9.2|
|unemployed|female|active|        12.4| 0.5| 10.8|
|unemployed|female| elder|        10.9| 0.4| 12.4|
|unemployed|female| young|        12.5| 0.2| 11.1|
|unemployed|  male|active|        11.4| 0.9| 11.4|
|unemployed|  male| elder|        10.7| 0.7| 12.3|
|unemployed|  male| young|        11.6| 0.2| 11.9|
+----------+------+------+------------+----+-----+
```

A) How much time do we spend on primary needs compared to other activities?

```
+-----------------+----------+
|avg(primaryNeeds)|avg(other)|
+-----------------+----------+
|           11.275|     10.15|
+-----------------+----------+
```

B) Do women and men spend the same amount of time in working?

```
+------+-----------------+
|   sex|        avg(work)|
+------+-----------------+
|female|2.083333333333333|
|  male|2.583333333333333|
+------+-----------------+
```

C) Does the time spent on primary needs change when people get older? In other words, how much time elder people allocate to leisure compared to active people?

```
+------+------------------+
|   Age|        avg(other)|
+------+------------------+
|active|             9.525|
| elder|10.649999999999999|
+------+------------------+
```

D) How much time do employed people spend on leisure compared to unemployed people?

```
+----------+-----------------+
|   working|       avg(other)|
+----------+-----------------+
|unemployed|            11.65|
|  employed|8.649999999999999|
+----------+-----------------+
```

### ON CASSANDRA

* eds7@DESKTOP-QBMRMI2:~/docker-hadoop$ docker ps
```
CONTAINER ID   IMAGE                                                    COMMAND                  CREATED       STATUS                    PORTS                                            NAMES
e7d0edd9b8ba   bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8          "/entrypoint.sh /run…"   6 weeks ago   Up 37 minutes (healthy)   0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp   namenode
587b96663542   bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8     "/entrypoint.sh /run…"   6 weeks ago   Up 37 minutes (healthy)   8188/tcp                                         historyserver
79058372f941   bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8   "/entrypoint.sh /run…"   6 weeks ago   Up 36 minutes (healthy)   8088/tcp                                         resourcemanager
927aaea87050   bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8       "/entrypoint.sh /run…"   6 weeks ago   Up 37 minutes (healthy)   8042/tcp                                         nodemanager
665f3753a5fc   bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8          "/entrypoint.sh /run…"   6 weeks ago   Up 37 minutes (healthy)   9864/tcp                                         datanode
```
* eds7@DESKTOP-QBMRMI2:~/docker-hadoop$ docker run -d --name cassandra -p 9042:9042 cassandra
```
1bbd42c29f176081a0bd71ea319340327e43cd2e0dd5da2e2a68f33dafc210fc
```
* eds7@DESKTOP-QBMRMI2:~/docker-hadoop$ docker ps
```
CONTAINER ID   IMAGE                                                    COMMAND                  CREATED         STATUS                    PORTS                                                       NAMES
1bbd42c29f17   cassandra                                                "docker-entrypoint.s…"   4 seconds ago   Up 3 seconds              7000-7001/tcp, 7199/tcp, 9160/tcp, 0.0.0.0:9042->9042/tcp   cassandra
e7d0edd9b8ba   bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8          "/entrypoint.sh /run…"   6 weeks ago     Up 37 minutes (healthy)   0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp              namenode
587b96663542   bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8     "/entrypoint.sh /run…"   6 weeks ago     Up 37 minutes (healthy)   8188/tcp                                                    historyserver
79058372f941   bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8   "/entrypoint.sh /run…"   6 weeks ago     Up 37 minutes (healthy)   8088/tcp                                                    resourcemanager
927aaea87050   bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8       "/entrypoint.sh /run…"   6 weeks ago     Up 37 minutes (healthy)   8042/tcp                                                    nodemanager
665f3753a5fc   bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8          "/entrypoint.sh /run…"   6 weeks ago     Up 37 minutes (healthy)   9864/tcp                                                    datanode
```
* eds7@DESKTOP-QBMRMI2:~/docker-hadoop$ docker exec -it cassandra bash
```
root@1bbd42c29f17:/# cqlsh
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
root@1bbd42c29f17:/# cqlsh
Connected to Test Cluster at 127.0.0.1:9042
[cqlsh 6.1.0 | Cassandra 4.1.0 | CQL spec 3.4.6 | Native protocol v5]
Use HELP for help.
```
```
cqlsh> CREATE KEYSPACE IF NOT EXISTS time_usage WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
cqlsh> CREATE KEYSPACE IF NOT EXISTS time_use_survey WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
cqlsh> CREATE TABLE IF NOT EXISTS time_usage.summary (
t_updat   ... uuid text,
e_time   ... last_update_timestamp timestamp,
ng text,
sex tex   ... working text,
t,
age text,
pri   ... sex text,
   ... age text,
   ... primary_needs double,
,
other double,
PRIM   ... work double,
ARY K   ... other double,
   ... PRIMARY KEY (uuid)
   ...
cqlsh> CREATE TABLE IF NOT EXISTS time_use_survey (
   ... id text,
   ... working texti
   ... working texti
cqlsh> CREATE TABLE IF NOT EXISTS time_use_survey (id varchar, working varchar, sex varchar, age varchar, primary_needs double,
   ... work double, other double, PRIMARY KEY(id));
InvalidRequest: Error from server: code=2200 [Invalid query] message="No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"
cqlsh> CREATE TABLE IF NOT EXISTS time_use_survey.summary (id varchar, working varchar, sex varchar, age varchar, primary_needs double, work double, other double, PRIMARY KEY(id));
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other)
   ... VALUES ('001', 'employed','female','active',11.5, 4.2, 8.1);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other)
ES ('00   ... VALUES ('001', 'employed','female','active',11.5, 4.2, 8.1);ork, other)
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other)
VALUES ('   ...    ... VALUES ('002', 'employed','female','elder',10.6, 3.9, 9.3);
SyntaxException: line 2:3 mismatched input '..' expecting K_VALUES (... primary_needs, work, other)   [..]...)
```
```
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('002', 'employed','female','elder',10.6, 3.9, 9.3);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('003', 'employed','female','young',11.6, 3.3, 8.9);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('004', 'employed','male','active',10.8, 5.2, 7.8);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('005', 'employed','male','elder',10.4, 4.8, 8.6);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('006', 'employed','male','young',10.9, 3.7, 9.2);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('007', 'unemployed','female','active',12.4, 0.5, 10.8);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('008', 'unemployed','female','elder',10.9, 0.4, 12.4);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('009', 'unemployed','female','young',12.5, 0.2, 11.1);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('010', 'unemployed','male','active',11.4, 0.9, 11.4);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('011', 'unemployed','male','elder',10.7, 0.7, 12.3);
cqlsh> INSERT INTO time_use_survey.summary (id,working,sex, age, primary_needs, work, other) VALUES ('012', 'unemployed','male','young',11.6, 0.2, 11.9);
```
```
cqlsh> SELECT * FROM time_use_survey
   ...
cqlsh> SELECT * FROM time_use_survey.summary
   ...
```
```
cqlsh> SELECT * FROM time_use_survey.summary;

 id  | age    | other | primary_needs | sex    | work | working
-----+--------+-------+---------------+--------+------+------------
 004 | active |   7.8 |          10.8 |   male |  5.2 |   employed
 007 | active |  10.8 |          12.4 | female |  0.5 | unemployed
 011 |  elder |  12.3 |          10.7 |   male |  0.7 | unemployed
 005 |  elder |   8.6 |          10.4 |   male |  4.8 |   employed
 012 |  young |  11.9 |          11.6 |   male |  0.2 | unemployed
 010 | active |  11.4 |          11.4 |   male |  0.9 | unemployed
 002 |  elder |   9.3 |          10.6 | female |  3.9 |   employed
 001 | active |   8.1 |          11.5 | female |  4.2 |   employed
 009 |  young |  11.1 |          12.5 | female |  0.2 | unemployed
 006 |  young |   9.2 |          10.9 |   male |  3.7 |   employed
 003 |  young |   8.9 |          11.6 | female |  3.3 |   employed
 008 |  elder |  12.4 |          10.9 | female |  0.4 | unemployed

(12 rows)
```
```
cqlsh> SELECT AVG(primary_needs), AVG(other) FROM time_use_survey.summary;

 system.avg(primary_needs) | system.avg(other)
---------------------------+-------------------
                    11.275 |             10.15

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT sex, AVG(work) FROM time_use_survey.summary WHERE sex = 'female' ALLOW FILTERING;

 sex    | system.avg(work)
--------+------------------
 female |          2.08333

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT sex, AVG(work) FROM time_use_survey.summary WHERE sex = '  male' ALLOW FILTERING;

 sex  | system.avg(work)
------+------------------
 null |                0

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT sex, AVG(work) FROM time_use_survey.summary WHERE sex = 'male' ALLOW FILTERING;

 sex  | system.avg(work)
------+------------------
 male |          2.58333

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT age, AVG(other) FROM time_usage.summary WHERE age = 'active' ALLOW FILTERING;
InvalidRequest: Error from server: code=2200 [Invalid query] message="table summary does not exist"
cqlsh> SELECT age, AVG(other) FROM time_use_survey.summary WHERE age = 'active' ALLOW FILTERING;

 age    | system.avg(other)
--------+-------------------
 active |             9.525

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT age, AVG(other) FROM time_use_survey.summary WHERE age = 'elder' ALLOW FILTERING;

 age   | system.avg(other)
-------+-------------------
 elder |             10.65

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT working, AVG(other) FROM time_use_survey.summary WHERE working = 'working' ALLOW FILTERING;

 working | system.avg(other)
---------+-------------------
    null |                 0

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh> SELECT working, AVG(other) FROM time_use_survey.summary WHERE working = 'employed' ALLOW FILTERING;

 working  | system.avg(other)
----------+-------------------
 employed |              8.65

(1 rows)

Warnings :
Aggregation query used without partition key
```
```
cqlsh>
cqlsh> SELECT working, AVG(other) FROM time_use_survey.summary WHERE working = 'unemployed' ALLOW FILTERING;

 working    | system.avg(other)
------------+-------------------
 unemployed |             11.65

(1 rows)

Warnings :
Aggregation query used without partition key
```

## REFERENCES
https://www.kaggle.com/datasets/bls/american-time-use-survey
