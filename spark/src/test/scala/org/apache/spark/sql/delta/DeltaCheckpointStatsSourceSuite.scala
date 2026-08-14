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

import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DataSkippingDeltaTestsUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

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

  // Same matrix again, but with state reconstruction carrying typed statistics through log replay
  // instead of json encoding them. Results and skipping must be identical to the json path.
  Seq(
    (true, false),
    (false, true),
    (true, true)
  ).foreach { case (asJson, asStruct) =>
    test("parsed stats passthrough preserves skipping: " +
        s"writeStatsAsJson=$asJson, writeStatsAsStruct=$asStruct") {
      withSQLConf(
          DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> "true") {
        withCheckpointedTable(asJson, asStruct) { (deltaLog, path) =>
          checkAnswer(
            spark.read.format("delta").load(path).where("id = 25").select("id"),
            Row(25L))
          assert(spark.read.format("delta").load(path).count() === numFiles * 10)
          assert(filesRead(spark, deltaLog, "id = 25", checkEmptyUnusedFilters = true) === 1)
          // Consumers that need json statistics (numRecords, tightBounds, ...) must still get
          // them, whichever representation the checkpoint provided.
          assert(deltaLog.update().allFiles.collect().forall(_.numLogicalRecords.contains(10L)))
        }
      }
    }
  }

  test("parsed stats passthrough agrees with the json path on the stats themselves") {
    def statsFor(passthrough: Boolean, asJson: Boolean, asStruct: Boolean): Seq[Row] = {
      var result: Seq[Row] = Seq.empty
      val passthroughConf =
        DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> passthrough.toString
      withSQLConf(passthroughConf) {
        withCheckpointedTable(asJson, asStruct) { (deltaLog, _) =>
          result = deltaLog.update().withStats.select(col("stats")).collect().toSeq
        }
      }
      result.sortBy(_.toString)
    }

    // Whether the statistics travelled as a struct or as json, data skipping must see the same
    // values.
    val baseline = statsFor(passthrough = false, asJson = true, asStruct = false)
    assert(baseline.length === numFiles)
    Seq((true, false), (false, true), (true, true)).foreach { case (asJson, asStruct) =>
      assert(statsFor(passthrough = true, asJson = asJson, asStruct = asStruct) === baseline,
        s"Parsed stats passthrough changed the statistics for " +
          s"writeStatsAsJson=$asJson, writeStatsAsStruct=$asStruct")
    }
  }

  test("parsed stats passthrough handles a checkpoint whose stats schema predates the table") {
    withSQLConf(DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        writeFilesWithDisjointRanges(path)
        val deltaLog = DeltaLog.forTable(spark, dir)
        checkpointWithStatsProperties(
          deltaLog, writeStatsAsJson = false, writeStatsAsStruct = true)

        // The checkpoint's stats_parsed schema no longer matches the table's stats schema, so the
        // extra field has to be null-filled rather than breaking the read.
        sql(s"ALTER TABLE delta.`$path` ADD COLUMN (extra STRING)")
        DeltaLog.clearCache()

        val refreshed = DeltaLog.forTable(spark, dir)
        assert(filesRead(spark, refreshed, "id = 25", checkEmptyUnusedFilters = true) === 1)
        checkAnswer(
          spark.read.format("delta").load(path).where("id = 25").select("id", "extra"),
          Row(25L, null))
      }
    }
  }

  test("a checkpoint written from typed state keeps its statistics") {
    // Checkpoint writing reads `add.stats` off the state, so a state carrying typed statistics has
    // to materialize the json form for it. Otherwise the next checkpoint silently loses all stats.
    withSQLConf(DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        writeFilesWithDisjointRanges(path)
        val deltaLog = DeltaLog.forTable(spark, dir)
        checkpointWithStatsProperties(
          deltaLog, writeStatsAsJson = false, writeStatsAsStruct = true)

        // Re-checkpoint from the typed state, this time asking for json statistics as well.
        val refreshed = DeltaLog.forTable(spark, dir)
        refreshed.update()
        checkpointWithStatsProperties(
          refreshed, writeStatsAsJson = true, writeStatsAsStruct = true)

        val reread = DeltaLog.forTable(spark, dir)
        assert(reread.update().allFiles.collect().forall(_.stats != null),
          "The checkpoint written from typed state lost its json statistics")
        assert(filesRead(spark, reread, "id = 25", checkEmptyUnusedFilters = true) === 1)
      }
    }
  }

  test("parsed stats passthrough is skipped for tables without stats") {
    // An empty stats schema has no typed column to carry, so the state must stay unchanged.
    withSQLConf(DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        writeFilesWithDisjointRanges(path)
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
          s"('delta.dataSkippingNumIndexedCols' = '0')")
        val deltaLog = DeltaLog.forTable(spark, dir)
        checkpointWithStatsProperties(
          deltaLog, writeStatsAsJson = false, writeStatsAsStruct = true)

        checkAnswer(
          spark.read.format("delta").load(path).where("id = 25").select("id"),
          Row(25L))
        assert(spark.read.format("delta").load(path).count() === numFiles * 10)
      }
    }
  }

  test("parsed stats passthrough persists the statistics only once") {
    // Delta persists file statistics twice for any snapshot doing data skipping: once as json in
    // the state cache, and once as a struct in the data skipping cache. Carrying them typed makes
    // the second one redundant, which is the largest practical win of the passthrough.
    def persistedRddCount(passthrough: Boolean): Int = {
      var count = 0
      val passthroughConf =
        DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> passthrough.toString
      withSQLConf(passthroughConf) {
        withCheckpointedTable(writeStatsAsJson = false, writeStatsAsStruct = true) {
          (deltaLog, _) =>
            spark.sparkContext.getPersistentRDDs.values.foreach(_.unpersist(blocking = true))
            val snapshot = deltaLog.update()
            // Materialize both the state and the files-with-statistics view.
            snapshot.stateDF.collect()
            snapshot.withStats.collect()
            count = spark.sparkContext.getPersistentRDDs.size
        }
      }
      count
    }

    val withoutPassthrough = persistedRddCount(passthrough = false)
    val withPassthrough = persistedRddCount(passthrough = true)
    assert(withoutPassthrough === 2,
      s"Expected the state and the stats to be persisted separately, got $withoutPassthrough")
    assert(withPassthrough === 1,
      s"Expected a single persisted copy of the statistics, got $withPassthrough")
  }

  test("preferring parsed stats keeps skipping and converts nothing") {
    // The common configuration: the checkpoint carries both representations. Data skipping should
    // read the typed one, while the consumers that need json still get it straight from the
    // checkpoint, so neither is converted.
    withSQLConf(
        DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_SNAPSHOT_PREFER_PARSED_STATS_ENABLED.key -> "true") {
      withCheckpointedTable(writeStatsAsJson = true, writeStatsAsStruct = true) {
        (deltaLog, path) =>
          val statsSources = collectStatsSources {
            deltaLog.update().stateDF.collect()
          }
          assert(statsSources.nonEmpty)
          assert(statsSources.forall(_ === CheckpointProvider.StatsSource
            .StatsParsedPreferredOverJson),
            s"Expected the typed statistics to win, got ${statsSources.mkString(", ")}")

          checkAnswer(
            spark.read.format("delta").load(path).where("id = 25").select("id"),
            Row(25L))
          assert(filesRead(spark, deltaLog, "id = 25", checkEmptyUnusedFilters = true) === 1)
          // The json statistics are still available to the consumers that need them.
          assert(deltaLog.update().allFiles.collect().forall(_.numLogicalRecords.contains(10L)))
      }
    }
  }

  test("preferring parsed stats agrees with the json path on the stats themselves") {
    def stats(prefer: Boolean): Seq[Row] = {
      var result: Seq[Row] = Seq.empty
      withSQLConf(
          DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> prefer.toString,
          DeltaSQLConf.DELTA_SNAPSHOT_PREFER_PARSED_STATS_ENABLED.key -> prefer.toString) {
        withCheckpointedTable(writeStatsAsJson = true, writeStatsAsStruct = true) { (deltaLog, _) =>
          result = deltaLog.update().withStats.select(col("stats")).collect().toSeq
        }
      }
      result.sortBy(_.toString)
    }
    assert(stats(prefer = true) === stats(prefer = false),
      "Preferring the typed statistics changed what data skipping sees")
  }

  test("preferring parsed stats has no effect without the passthrough") {
    // On its own the preference would be a pessimization, since the typed statistics would have to
    // be json encoded for consumers that only understand json.
    withSQLConf(
        DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> "false",
        DeltaSQLConf.DELTA_SNAPSHOT_PREFER_PARSED_STATS_ENABLED.key -> "true") {
      withCheckpointedTable(writeStatsAsJson = true, writeStatsAsStruct = true) { (deltaLog, _) =>
        val statsSources = collectStatsSources {
          deltaLog.update().stateDF.collect()
        }
        assert(statsSources.forall(_ === CheckpointProvider.StatsSource
          .JsonPreferredOverStatsParsed))
      }
    }
  }

  test("variant statistics survive the typed path in both representations") {
    // Variant statistics are Z85 encoded strings in json but real variants in a struct, so the
    // typed path has to re-encode them on the way back to json. Getting that wrong would write a
    // corrupt `stats` column into the next checkpoint, so compare against the json path directly.
    withSQLConf(
        "spark.sql.variant.writeShredding.enabled" -> "true",
        SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true",
        DeltaSQLConf.COLLECT_VARIANT_DATA_SKIPPING_STATS.key -> "true",
        DeltaSQLConf.DELTA_STATS_LIMIT_PER_VARIANT.key -> "10") {
      def jsonStatsOfVariantTable(passthrough: Boolean, asStruct: Boolean): Seq[String] = {
        var result: Seq[String] = Seq.empty
        val passthroughConf =
          DeltaSQLConf.DELTA_SNAPSHOT_PARSED_STATS_PASSTHROUGH_ENABLED.key -> passthrough.toString
        withSQLConf(passthroughConf) {
          withTempDir { dir =>
            val path = dir.getCanonicalPath
            spark.sql(
              "SELECT id, parse_json(concat('{\"k\": ', id, '}')) AS v FROM range(0, 10)")
              .repartition(1)
              .write.format("delta")
              .option("delta.enableVariantShredding", "true")
              .save(path)
            val deltaLog = DeltaLog.forTable(spark, dir)
            checkpointWithStatsProperties(
              deltaLog,
              writeStatsAsJson = !asStruct,
              writeStatsAsStruct = asStruct)
            val refreshed = DeltaLog.forTable(spark, dir)
            val snapshot = refreshed.update()
            // Guard against this test quietly becoming vacuous: the Z85 re-encoding only matters
            // if variant columns really made it into the stats schema.
            assert(
              SchemaUtils.checkForVariantTypeColumnsRecursively(snapshot.statsSchema),
              s"Expected variant statistics, got ${snapshot.statsSchema.treeString}")
            // Correct results still come back.
            checkAnswer(
              spark.read.format("delta").load(path).where("id = 5").select("id"),
              Row(5L))
            result = snapshot.allFiles.collect().map(_.stats).toSeq
          }
        }
        result.sorted
      }

      val viaJson = jsonStatsOfVariantTable(passthrough = false, asStruct = false)
      assert(viaJson.nonEmpty && viaJson.forall(s => s != null && s.nonEmpty))
      assert(jsonStatsOfVariantTable(passthrough = true, asStruct = true) === viaJson,
        "Variant statistics carried typed do not round trip back to the same json")
    }
  }

  // Reconciling a checkpoint's stats_parsed schema with the snapshot's stats schema has to
  // reproduce what from_json does implicitly on the json path, and refuse anything it cannot
  // represent faithfully. These cases are what stands between a schema change and wrong bounds.
  private def reconcile(source: StructType, target: StructType): Option[Column] =
    CheckpointProvider.reconcileParsedStatsSchema(col("stats_parsed"), source, target)

  test("stats schema reconciliation: identical schemas") {
    val schema = new StructType().add("numRecords", LongType)
      .add("minValues", new StructType().add("id", LongType))
    assert(reconcile(schema, schema).isDefined)
  }

  test("stats schema reconciliation: field the checkpoint does not have is null filled") {
    val source = new StructType().add("numRecords", LongType)
    val target = new StructType().add("numRecords", LongType)
      .add("minValues", new StructType().add("added", StringType))
    assert(reconcile(source, target).isDefined)
  }

  test("stats schema reconciliation: field only the checkpoint has is dropped") {
    val source = new StructType().add("numRecords", LongType)
      .add("minValues", new StructType().add("id", LongType).add("dropped", StringType))
    val target = new StructType().add("numRecords", LongType)
      .add("minValues", new StructType().add("id", LongType))
    assert(reconcile(source, target).isDefined)
  }

  test("stats schema reconciliation: widened types are up-cast") {
    val source = new StructType().add("minValues", new StructType().add("id", IntegerType))
    val target = new StructType().add("minValues", new StructType().add("id", LongType))
    assert(reconcile(source, target).isDefined)
  }

  test("stats schema reconciliation: narrowing types fall back") {
    // Reading a long minimum as an int would silently corrupt the bounds.
    val source = new StructType().add("minValues", new StructType().add("id", LongType))
    val target = new StructType().add("minValues", new StructType().add("id", IntegerType))
    assert(reconcile(source, target).isEmpty)
  }

  test("stats schema reconciliation: a field that stopped being a struct falls back") {
    val source = new StructType()
      .add("minValues", new StructType().add("c", new StructType().add("x", LongType)))
    val target = new StructType().add("minValues", new StructType().add("c", LongType))
    assert(reconcile(source, target).isEmpty)
  }

  test("stats schema reconciliation: a field that became a struct falls back") {
    val source = new StructType().add("minValues", new StructType().add("c", LongType))
    val target = new StructType()
      .add("minValues", new StructType().add("c", new StructType().add("x", LongType)))
    assert(reconcile(source, target).isEmpty)
  }

  test("stats schema reconciliation: an unreconcilable nested field fails the whole struct") {
    // Falling back wholesale is the point: a partially reinterpreted struct would mix trustworthy
    // and untrustworthy bounds with no way for the caller to tell them apart.
    val source = new StructType().add("numRecords", LongType)
      .add("minValues", new StructType().add("ok", LongType).add("bad", LongType))
    val target = new StructType().add("numRecords", LongType)
      .add("minValues", new StructType().add("ok", LongType).add("bad", IntegerType))
    assert(reconcile(source, target).isEmpty)
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
