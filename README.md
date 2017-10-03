# Spark SQL Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-sql-perf.svg)](https://travis-ci.org/databricks/spark-sql-perf)

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.6+.

**Note: This README is still under development. Please also check our source code for more information.**

# Quick Start

```
$ bin/run --help

spark-sql-perf 0.2.0
Usage: spark-sql-perf [options]

  -b <value> | --benchmark <value>
        the name of the benchmark to run
  -f <value> | --filter <value>
        a filter on the name of the queries to run
  -i <value> | --iterations <value>
        the number of iterations to run
  --help
        prints this usage text
        
$ bin/run --benchmark DatasetPerformance
```

# TPC-DS

## How to use it
The rest of document will use TPC-DS benchmark as an example. We will add contents to explain how to use other benchmarks add the support of a new benchmark dataset in future.

### Setup a benchmark
Before running any query, a dataset needs to be setup by creating a `Benchmark` object. Generating
the TPCDS data requires dsdgen built and available on the machines. We have a fork of dsdgen that
you will need. It can be found [here](https://github.com/davies/tpcds-kit).  

```
// OPTIONAL: If not done already, you have to set the path for the results
// spark.config("spark.sql.perf.results", "/tmp/results")

import com.databricks.spark.sql.perf.tpcds.Tables
// Tables in TPC-DS benchmark used by experiments.
// dsdgenDir is the location of dsdgen tool installed in your machines.
// scaleFactor defines the size of the dataset to generate (in GB)
val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)

// Generate data.
// location is the place there the generated data will be written
// format is a valid spark format like "parquet"
tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(location, format, databaseName, overwrite)
// Or, if you want to create temporary tables
tables.createTemporaryTables(location, format)
// Setup TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
```

### Run benchmarking queries

The following JARs are required to be included in your Spark job classpath.
```
scala-logging-api_2.11-2.1.2.jar
scala-logging-slf4j_2.11-2.1.2.jar
```

After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
import com.databricks.spark.sql.perf.tpcds.TPCDS

val tpcds = new TPCDS (sqlContext = sqlContext)
// Set:
val resultLocation = ... // place to write results
val iterations = 1 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.
// Run:
val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)
```

For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `resultLocation` (for example `/tmp/tpcds_results/timestamp=1506369471599`). The performance results are stored in the JSON format.

### Retrieve results
While the experiment is running you can use `experiment.html` to list the status.  Once the experiment is complete, you can load the results from HDFS or disk.

```
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val resultLocation = "hdfs:///tmp/tpcds_results" // place where results were stored from previous step
val resultTable = spark.read.json(resultLocation)
resultTable.createOrReplaceTempView("sqlPerformance")
val df = sqlContext.table("sqlPerformance").filter("timestamp = 1506369471599").select($"results")
df.show()
df.printSchema()
val df2 = df.withColumn("tpcds_all_queries", explode(col("results")))
df2.select("tpcds_all_queries.name","tpcds_all_queries.executionTime").collect().foreach(println)
df2.withColumn("Name", substring(col("tpcds_all_queries.name"), 2, 100)).withColumn("Runtime", (col("tpcds_all_queries.parsingTime") + col("tpcds_all_queries.analysisTime") + col("tpcds_all_queries.optimizationTime") + col("tpcds_all_queries.planningTime") + col("tpcds_all_queries.executionTime")) / 1000.0).select("Name", "Runtime").collect().foreach(println)
```
