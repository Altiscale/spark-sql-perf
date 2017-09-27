/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import org.apache.commons.io.IOUtils

import com.databricks.spark.sql.perf.{Benchmark, ExecutionMode, Query}

/**
 * This implements the official TPCDS v2.4 queries with only cosmetic modifications.
 */
trait Tpcds_2_4_70_Queries extends Benchmark {

  import ExecutionMode._

  private val queryNames = Seq(
    "q70", "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79",
    "q80", "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89",
    "q90", "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99",
    "ss_max"
  )

  val tpcds2_4_70Queries = queryNames.map { queryName =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpcds_2_4/$queryName.sql"))
    Query(queryName + "-v2.4", queryContent, description = "TPCDS 2.4 Query",
      executionMode = CollectResults)
  }

  val tpcds2_4_70QueriesMap = tpcds2_4_70Queries.map(q => q.name.split("-").get(0) -> q).toMap
}
