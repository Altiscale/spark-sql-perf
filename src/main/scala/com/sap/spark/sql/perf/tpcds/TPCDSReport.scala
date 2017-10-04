package com.sap.spark.sql.perf.tpcds

import scala.collection.mutable

import com.databricks.spark.sql.perf._

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object TPCDSReport {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(s"""
        |Usage: TPCDSReport <timestamp> <tpcds_result_on_hdfs_loc>
        |  <timestamp> recorded by tpcds benchmark in epoch time
        |  <tpcds_result_on_hdfs_loc> the parent hdfs location for the tpcds results
        |  sub-directory is the timestamp format. e.g. /tmp/tpcds_results/timestamp=1506369471599
        |  where tpcds_result_on_hdfs_loc is /tmp/tpcds_results and timestamp is 1506369471599
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(tstamp, res_hdfs_loc) = args

    val spark = SparkSession
      .builder
      .appName("TPCDS Reporting Tool - v2.4")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val resultLocation = "hdfs://" + res_hdfs_loc
    val resultTable = spark.read.json(resultLocation)
    resultTable.createOrReplaceTempView("sqlPerformance")
    // sqlContext.table("sqlPerformance")
    val df = sqlContext.table("sqlPerformance").filter("timestamp = " + tstamp).select("results")
    df.show()
    df.printSchema()
    val df2 = df.withColumn("tpcds_all_queries", explode(col("results")))
    df2.select("tpcds_all_queries.name","tpcds_all_queries.executionTime").collect().foreach(println)
    df2.withColumn("Name", substring(col("tpcds_all_queries.name"), 2, 100)).withColumn("Runtime", (col("tpcds_all_queries.parsingTime") + col("tpcds_all_queries.analysisTime") + col("tpcds_all_queries.optimizationTime") + col("tpcds_all_queries.planningTime") + col("tpcds_all_queries.executionTime")) / 1000.0).select("Name", "Runtime").collect().foreach(println)
  }
}

