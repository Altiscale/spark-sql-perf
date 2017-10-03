val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import com.databricks.spark.sql.perf.tpcds.TPCDS
sqlContext.sql(s"USE TARGETDATABASE");
val tpcds = new TPCDS (sqlContext = sqlContext)
val resultLocation = "hdfs://HDFSDEST/QUERYIDX" // results will be on HDFS
val iterations = 1 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4_QUERYIDXQueries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.
val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)

experiment.waitForFinish(timeout)
