val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import com.databricks.spark.sql.perf.tpcds.TPCDSdc

val usrDatabase = spark.conf.get("spark.sql.perf.tpcds.database") 

val hdfsLoc = spark.conf.get("spark.sql.perf.tpcds.results") 

sqlContext.sql("USE " + usrDatabase);

val tpcds = new TPCDSdc (sqlContext = sqlContext)

val resultLocation = "hdfs://" + hdfsLoc + "/QUERYIDX" // results will be on HDFS

val iterations = 1 // how many iterations of queries to run.

val queries = tpcds.tpcds2_4_QUERYIDXQueries // queries to run.

val timeout = 24*60*60 // timeout, in seconds.

val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)

experiment.waitForFinish(timeout)

