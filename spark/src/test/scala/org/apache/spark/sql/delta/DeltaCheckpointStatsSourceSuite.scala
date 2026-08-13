/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.stats.DataSkippingDeltaTestsUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for how state reconstruction picks between a checkpoint's json `add.stats` column and its
 * typed `add.stats_parsed` column, and for the fidelity of the `to_json` -> `from_json` round trip
 * that the `stats_parsed`-only path currently performs.
 *
 * These tests pin down the *current* behavior across the full
 * `writeStatsAsJson` x `writeStatsAsStruct` matrix so that changes to the stats representation
 * carried through state reconstruction cannot silently regress data skipping. See
 * https://github.com/delta-io/delta/issues/7459.
 */
class DeltaCheckpointStatsSourceSuite
  extends QueryTest
  with SharedSparkSession
  with DataSkippingDeltaTestsUtils
  with DeltaColumnMappingTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  /** Number of files written by [[writeFilesWithDisjointRanges]]. */
  private val numFiles = 5

  /**
   * Writes `numFiles` files, each holding a disjoint range of `id` values, so that a point lookup
   * can only avoid reading every file if data skipping has usable statistics.
   */
  private def writeFilesWithDisjointRanges(path: String): Unit = {
    (0 until numFiles).foreach { i =>
      val mode = if (i == 0) "overwrite" else "append"
      spark.range(i * 10, i * 10 + 10)
        .withColumn("part", 'id % 2)
        // One file per write, so that the file count (and therefore the number of files a point
        // lookup must read) is deterministic regardless of local parallelism.
        .repartition(1)
        .write.format("delta").mode(mode).save(path)
    }
  }

  /**
   * Sets the checkpoint stats table properties, writes a checkpoint, and drops cached state so
   * that the following read has to reconstruct the snapshot from the checkpoint we just wrote.
   */
  private def checkpointWithStatsProperties(
      deltaLog: DeltaLog,
      writeStatsAsJson: Boolean,
      writeStatsAsStruct: Boolean): Unit = {
    sql(
      s"ALTER TABLE delta.`${deltaLog.dataPath}` SET TBLPROPERTIES (" +
        s"delta.checkpoint.writeStatsAsJson = $writeStatsAsJson, " +
        s"delta.checkpoint.writeStatsAsStruct = $writeStatsAsStruct)")
    deltaLog.checkpoint()
    DeltaLog.clearCache()
  }

  /** Reads back the stats source usage logs emitted while `f` runs. */
  private def collectStatsSources(f: => Unit): Seq[String] = {
    DeltaTestUtils.collectUsageLogs(CheckpointProvider.STATS_SOURCE_OP_TYPE)(f)
      .map { r =>
        val blob = JsonUtils.fromJson[Map[String, Any]](r.blob)
        blob("statsSource").toString
      }
      .toSeq
  }

  /**
   * Runs `f` against a table whose latest checkpoint was written with the given stats properties.
   * `f` receives a freshly resolved [[DeltaLog]] whose snapshot is backed by that checkpoint.
   */
  private def withCheckpointedTable(
      writeStatsAsJson: Boolean,
      writeStatsAsStruct: Boolean)(f: (DeltaLog, String) => Unit): Unit = {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      writeFilesWithDisjointRanges(path)
      val deltaLog = DeltaLog.forTable(spark, dir)
      checkpointWithStatsProperties(deltaLog, writeStatsAsJson, writeStatsAsStruct)
      f(DeltaLog.forTable(spark, dir), path)
    }
  }

  /**
   * Writes `df` as a fresh table, checkpoints it with the given stats properties, and returns the
   * single row of statistics that data skipping would see for the resulting file.
   */
  private def singleFileStats(
      df: DataFrame,
      writeStatsAsJson: Boolean,
      writeStatsAsStruct: Boolean): Row = {
    var result: Row = null
    withTempDir { dir =>
      df.write.format("delta").save(dir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, dir)
      checkpointWithStatsProperties(deltaLog, writeStatsAsJson, writeStatsAsStruct)
      result = DeltaLog.forTable(spark, dir).update().withStats.select("stats").head()
    }
    result
  }

  // The full matrix from the issue: which stats representation the checkpoint carries, whether
  // data skipping still works, and which source state reconstruction reports having used.
  Seq(
    // (writeStatsAsJson, writeStatsAsStruct, skippingWorks, expectedStatsSource)
    (true, false, true, CheckpointProvider.StatsSource.Json),
    (false, true, true, CheckpointProvider.StatsSource.StatsParsed),
    (true, true, true, CheckpointProvider.StatsSource.JsonPreferredOverStatsParsed),
    (false, false, false, CheckpointProvider.StatsSource.NoStats)
  ).foreach { case (asJson, asStruct, skippingWorks, expectedStatsSource) =>
    test("stats source matrix: " +
        s"writeStatsAsJson=$asJson, writeStatsAsStruct=$asStruct") {
      withCheckpointedTable(asJson, asStruct) { (deltaLog, path) =>
        // Results must be correct no matter which stats representation we read.
        checkAnswer(
          spark.read.format("delta").load(path).where("id = 25").select("id"),
          Row(25L))
        assert(spark.read.format("delta").load(path).count() === numFiles * 10)

        // Data skipping must still work whenever the checkpoint carries stats in any form.
        val filesReadForPointLookup = filesRead(
          spark, deltaLog, "id = 25", checkEmptyUnusedFilters = true)
        if (skippingWorks) {
          assert(filesReadForPointLookup === 1,
            s"Expected to skip all but one file, read $filesReadForPointLookup")
        } else {
          assert(filesReadForPointLookup === numFiles,
            "Without stats every file must be read")
        }
      }
    }

    test("stats source is reported in usage logs: " +
        s"writeStatsAsJson=$asJson, writeStatsAsStruct=$asStruct") {
      withCheckpointedTable(asJson, asStruct) { (deltaLog, _) =>
        val statsSources = collectStatsSources {
          deltaLog.update().stateDF.collect()
        }
        assert(statsSources.nonEmpty, "Expected a stats source usage log")
        assert(statsSources.forall(_ === expectedStatsSource),
          s"Expected $expectedStatsSource but got ${statsSources.mkString(", ")}")
      }
    }
  }

  test("stats_parsed and json checkpoints agree on the stats seen by data skipping") {
    // The stats_parsed-only path currently json-encodes the struct back into `stats`, which data
    // skipping then parses again. Reading the same data through both paths must produce identical
    // statistics, otherwise that round trip is lossy.
    def statsForCheckpoint(writeStatsAsJson: Boolean, writeStatsAsStruct: Boolean): Seq[Row] = {
      var result: Seq[Row] = Seq.empty
      withCheckpointedTable(writeStatsAsJson, writeStatsAsStruct) { (deltaLog, _) =>
        result = deltaLog.update().withStats.select(col("stats")).collect().toSeq
      }
      // File paths differ between the two tables, so order by stats content instead.
      result.sortBy(_.toString)
    }

    val viaJson = statsForCheckpoint(writeStatsAsJson = true, writeStatsAsStruct = false)
    val viaStatsParsed = statsForCheckpoint(writeStatsAsJson = false, writeStatsAsStruct = true)
    assert(viaJson.length === numFiles)
    assert(viaJson === viaStatsParsed,
      "Statistics differ between the json and stats_parsed checkpoint paths")
  }

  test("round trip fidelity for skipping-eligible types") {
    // Types whose json rendering is not obviously an identity: timestamps and dates (time zone and
    // formatting), decimals (scale/precision), floats/doubles (special values), and long strings
    // (min/max truncation).
    val df = spark.sql(
      """SELECT
        |  CAST(1 AS LONG) AS c_long,
        |  CAST(1.5 AS FLOAT) AS c_float,
        |  CAST(2.5 AS DOUBLE) AS c_double,
        |  CAST(1.23456789 AS DECIMAL(20, 8)) AS c_decimal,
        |  CAST('2020-01-02 03:04:05.123' AS TIMESTAMP) AS c_timestamp,
        |  CAST('2020-01-02 03:04:05.123' AS TIMESTAMP_NTZ) AS c_timestamp_ntz,
        |  CAST('2020-01-02' AS DATE) AS c_date,
        |  'a string that is long enough to be truncated by min/max prefix logic' AS c_string,
        |  CAST(true AS BOOLEAN) AS c_boolean
        |""".stripMargin)

    def statsFor(writeStatsAsJson: Boolean, writeStatsAsStruct: Boolean): Row =
      singleFileStats(df, writeStatsAsJson, writeStatsAsStruct)

    assert(
      statsFor(writeStatsAsJson = true, writeStatsAsStruct = false) ===
        statsFor(writeStatsAsJson = false, writeStatsAsStruct = true),
      "Statistics differ between the json and stats_parsed checkpoint paths")
  }

  test("special float and double values survive the checkpoint round trip") {
    // NaN and +/-Infinity have no json literal representation, so they are rendered as strings.
    val df = Seq(
      (Float.NaN, Double.PositiveInfinity),
      (1.0f, Double.NegativeInfinity)).toDF("c_float", "c_double")

    def statsFor(writeStatsAsJson: Boolean, writeStatsAsStruct: Boolean): Row =
      singleFileStats(df, writeStatsAsJson, writeStatsAsStruct)

    assert(
      statsFor(writeStatsAsJson = true, writeStatsAsStruct = false) ===
        statsFor(writeStatsAsJson = false, writeStatsAsStruct = true),
      "Special float/double values differ between the json and stats_parsed checkpoint paths")
  }

  test("skipping works when a stats_parsed checkpoint is followed by json delta commits") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      writeFilesWithDisjointRanges(path)
      val deltaLog = DeltaLog.forTable(spark, dir)
      checkpointWithStatsProperties(
        deltaLog, writeStatsAsJson = false, writeStatsAsStruct = true)

      // Commits on top of the checkpoint always carry json stats, so the snapshot mixes both
      // representations and both must skip correctly.
      spark.range(100, 110).withColumn("part", 'id % 2)
        .repartition(1)
        .write.format("delta").mode("append").save(path)
      DeltaLog.clearCache()

      val refreshed = DeltaLog.forTable(spark, dir)
      checkAnswer(
        spark.read.format("delta").load(path).where("id = 105").select("id"),
        Row(105L))
      // One file from the checkpoint, one from the delta commit.
      assert(filesRead(spark, refreshed, "id = 25", checkEmptyUnusedFilters = true) === 1)
      assert(filesRead(spark, refreshed, "id = 105", checkEmptyUnusedFilters = true) === 1)
    }
  }

  test("adding a column after a stats_parsed checkpoint keeps skipping on existing columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      writeFilesWithDisjointRanges(path)
      val deltaLog = DeltaLog.forTable(spark, dir)
      checkpointWithStatsProperties(
        deltaLog, writeStatsAsJson = false, writeStatsAsStruct = true)

      // The checkpoint's stats_parsed schema now predates the table schema.
      sql(s"ALTER TABLE delta.`$path` ADD COLUMN (extra STRING)")
      DeltaLog.clearCache()

      val refreshed = DeltaLog.forTable(spark, dir)
      assert(filesRead(spark, refreshed, "id = 25", checkEmptyUnusedFilters = true) === 1)
      checkAnswer(
        spark.read.format("delta").load(path).where("id = 25").select("id", "extra"),
        Row(25L, null))
    }
  }

  test("no stats means no skipping but still correct results") {
    withCheckpointedTable(writeStatsAsJson = false, writeStatsAsStruct = false) {
      (deltaLog, path) =>
        // Every file must be read, and the query must still succeed.
        assert(filesRead(spark, deltaLog, "id = 25", checkEmptyUnusedFilters = true) === numFiles)
        checkAnswer(
          spark.read.format("delta").load(path).where("id = 25").select("id"),
          Row(25L))
    }
  }
}

class DeltaCheckpointStatsSourceNameColumnMappingSuite
  extends DeltaCheckpointStatsSourceSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "stats source matrix: writeStatsAsJson=false, writeStatsAsStruct=true",
    "stats source matrix: writeStatsAsJson=true, writeStatsAsStruct=true",
    "stats_parsed and json checkpoints agree on the stats seen by data skipping",
    "round trip fidelity for skipping-eligible types"
  )
}

class DeltaCheckpointStatsSourceIdColumnMappingSuite
  extends DeltaCheckpointStatsSourceSuite
  with DeltaColumnMappingEnableIdMode {

  override protected def runOnlyTests = Seq(
    "stats source matrix: writeStatsAsJson=false, writeStatsAsStruct=true",
    "stats source matrix: writeStatsAsJson=true, writeStatsAsStruct=true",
    "stats_parsed and json checkpoints agree on the stats seen by data skipping",
    "round trip fidelity for skipping-eligible types"
  )
}
