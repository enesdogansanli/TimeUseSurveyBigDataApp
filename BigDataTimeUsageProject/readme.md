# BIG DATA PROJECT


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

## QUESTIONS

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

