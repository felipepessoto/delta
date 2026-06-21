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

import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo}
import org.apache.spark.sql.delta.hooks.LogCompactionHook
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the writing side of log compaction files: the [[LogCompaction]] writer and the
 * [[LogCompactionHook]] post-commit hook that produces `<x>.<y>.compacted.json` files.
 *
 * The reading side (consuming compaction files for snapshot construction) is covered by
 * [[DeltaLogMinorCompactionSuite]].
 */
class LogCompactionSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  /** Returns the sorted (startVersion, endVersion) ranges of compaction files on disk. */
  private def compactedRanges(deltaLog: DeltaLog): Seq[(Long, Long)] = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    fs.listStatus(deltaLog.logPath)
      .filter(FileNames.isCompactedDeltaFile)
      .map(f => FileNames.compactedDeltaVersions(f.getPath))
      .sorted
      .toSeq
  }

  /** Returns the sorted versions of checkpoint files on disk. */
  private def checkpointVersions(deltaLog: DeltaLog): Seq[Long] = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    fs.listStatus(deltaLog.logPath)
      .filter(FileNames.isCheckpointFile)
      .map(f => FileNames.checkpointVersion(f.getPath))
      .distinct
      .sorted
      .toSeq
  }

  /** Appends a single row with the given value, producing one new commit. */
  private def appendRow(path: String, value: Long): Unit = {
    spark.range(value, value + 1).write.format("delta").mode("append").save(path)
  }

  /**
   * Appends rows until the table reaches `targetVersion` (inclusive). The committed values are
   * `0..targetVersion`, one per commit, so the table contents are `spark.range(targetVersion + 1)`.
   */
  private def commitUpToVersion(path: String, targetVersion: Long): DeltaLog = {
    (0L to targetVersion).foreach(v => appendRow(path, v))
    val deltaLog = DeltaLog.forTable(spark, path)
    assert(deltaLog.update().version === targetVersion)
    deltaLog
  }

  test("log compaction can be disabled") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES.key -> "false",
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 12)
        assert(compactedRanges(deltaLog).isEmpty,
          "no compaction files should be produced when the feature is disabled")
      }
    }
  }

  test("log compaction is enabled by default") {
    // Relies on the default `deltaLog.minorCompaction.useForWrites = true`; a checkpoint interval
    // larger than the compaction interval is still required for the hook to produce a compaction.
    withSQLConf(
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 10)
        assert(compactedRanges(deltaLog) === Seq((1L, 10L)),
          "compaction should be produced with the default configuration")
      }
    }
  }

  test("hook produces non-overlapping fixed windows at the configured interval") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES.key -> "true",
      DeltaConfigs.LOG_COMPACTION_INTERVAL.defaultTablePropertyKey -> "5",
      // High checkpoint interval so no checkpoint interferes with the windows.
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 10)
        // Triggered at v5 -> [1, 5] and v10 -> [6, 10]. The version 0 commit fills the [0, 0] gap.
        assert(compactedRanges(deltaLog) === Seq((1L, 5L), (6L, 10L)))
        assert(checkpointVersions(deltaLog).isEmpty)
      }
    }
  }

  test("produced compaction files are used for snapshot construction and preserve state") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES.key -> "true",
      DeltaConfigs.LOG_COMPACTION_INTERVAL.defaultTablePropertyKey -> "5",
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        commitUpToVersion(path, 10)

        DeltaLog.clearCache()
        val compactedSnapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
        val usedCompactions = compactedSnapshot.logSegment.deltas
          .map(_.getPath)
          .filter(FileNames.isCompactedDeltaFile)
          .map(p => FileNames.compactedDeltaVersions(p))
          .sorted
        assert(usedCompactions === Seq((1L, 5L), (6L, 10L)),
          "snapshot should be backed by the compaction files instead of individual commits")

        // The snapshot built from compaction files must match the one built from raw commits.
        withSQLConf(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key -> "false") {
          DeltaLog.clearCache()
          val rawSnapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
          assert(rawSnapshot.logSegment.deltas
            .forall(f => !FileNames.isCompactedDeltaFile(f.getPath)))
          assert(rawSnapshot.computeChecksum === compactedSnapshot.computeChecksum)
          checkAnswer(rawSnapshot.stateDF, compactedSnapshot.stateDF)
          checkAnswer(rawSnapshot.allFiles.toDF(), compactedSnapshot.allFiles.toDF())
        }

        checkAnswer(spark.read.format("delta").load(path), spark.range(11).toDF())
      }
    }
  }

  test("a checkpoint subsumes compaction and bounds the next window") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES.key -> "true",
      DeltaConfigs.LOG_COMPACTION_INTERVAL.defaultTablePropertyKey -> "5",
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "10") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 15)
        // v5  -> [1, 5]
        // v10 -> skipped, a checkpoint is written instead (checkpoint subsumes compaction)
        // v15 -> [11, 15], bounded below by checkpoint(10) + 1, not v15 - interval + 1 = 11
        assert(checkpointVersions(deltaLog).contains(10L))
        assert(compactedRanges(deltaLog) === Seq((1L, 5L), (11L, 15L)))

        // The snapshot should use the checkpoint at 10 plus the [11, 15] compaction.
        DeltaLog.clearCache()
        val snapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
        assert(snapshot.logSegment.checkpointProvider.version === 10L)
        val usedCompactions = snapshot.logSegment.deltas
          .map(_.getPath)
          .filter(FileNames.isCompactedDeltaFile)
          .map(p => FileNames.compactedDeltaVersions(p))
          .sorted
        assert(usedCompactions === Seq((11L, 15L)))
      }
    }
  }

  test("no compaction is produced before a full window of commits exists") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES.key -> "true",
      DeltaConfigs.LOG_COMPACTION_INTERVAL.defaultTablePropertyKey -> "5",
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        // Only versions 0..4 exist; the first interval boundary (v5) hasn't been reached.
        val deltaLog = commitUpToVersion(path, 4)
        assert(compactedRanges(deltaLog).isEmpty)
      }
    }
  }

  test("hook is registered on every transaction") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      appendRow(path, 0)
      val txn = DeltaLog.forTable(spark, path).startTransaction()
      assert(txn.containsPostCommitHook(LogCompactionHook))
    }
  }

  test("LogCompaction.compact writes a reconciled file without commitInfo") {
    withSQLConf(
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 5)

        LogCompaction.compact(deltaLog, deltaLog.update(), startVersion = 1, endVersion = 4)

        val compactedPath = FileNames.compactedDeltaFile(deltaLog.logPath, 1, 4)
        val actions = deltaLog.store
          .read(compactedPath, deltaLog.newDeltaHadoopConf())
          .map(Action.fromJson)
        // commitInfo actions are stripped during reconciliation, just like checkpoints.
        assert(actions.forall(!_.isInstanceOf[CommitInfo]))
        assert(actions.exists(_.isInstanceOf[AddFile]))
      }
    }
  }

  test("LogCompaction.compact is a no-op when the target file already exists") {
    withSQLConf(
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 5)
        val hadoopConf = deltaLog.newDeltaHadoopConf()
        val compactedPath = FileNames.compactedDeltaFile(deltaLog.logPath, 1, 4)

        // Pre-create the compaction file with sentinel content that real reconciliation would
        // never produce, so we can detect whether `compact` overwrote it.
        val sentinel = """{"sentinel":"do-not-overwrite"}"""
        deltaLog.store.write(compactedPath, Iterator(sentinel), overwrite = true, hadoopConf)

        LogCompaction.compact(deltaLog, deltaLog.update(), startVersion = 1, endVersion = 4)

        // The existing file must be left untouched: `compact` skips when the target exists.
        assert(deltaLog.store.read(compactedPath, hadoopConf).toSeq === Seq(sentinel),
          "an existing compaction file should not be recomputed or overwritten")
      }
    }
  }

  test("LogCompaction.compact emits telemetry for completed and skipped compactions") {
    withSQLConf(
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val deltaLog = commitUpToVersion(path, 5)
        val snapshot = deltaLog.update()

        // A completed compaction reports the range, commit/action counts, and file size.
        val completedLogs = DeltaTestUtils.collectUsageLogs("delta.logCompaction.stats") {
          LogCompaction.compact(deltaLog, snapshot, startVersion = 1, endVersion = 4)
        }
        assert(completedLogs.size === 1, "exactly one stats event should be emitted")
        val completed =
          JsonUtils.mapper.readValue[LogCompactionMetrics](completedLogs.head.blob)
        assert(completed.status === LogCompaction.STATUS_COMPLETED)
        assert(completed.skipReason.isEmpty)
        assert(completed.startVersion === 1 && completed.endVersion === 4)
        assert(completed.numCommitsCompacted === 4)
        assert(completed.numActions > 0, "the compaction should contain reconciled actions")
        assert(completed.compactedFileSizeBytes > 0, "the written file should have a size")
        assert(completed.durationMs >= 0)

        // Compacting the same range again is skipped with the target-already-exists reason.
        val skippedLogs = DeltaTestUtils.collectUsageLogs("delta.logCompaction.stats") {
          LogCompaction.compact(deltaLog, snapshot, startVersion = 1, endVersion = 4)
        }
        assert(skippedLogs.size === 1)
        val skipped = JsonUtils.mapper.readValue[LogCompactionMetrics](skippedLogs.head.blob)
        assert(skipped.status === LogCompaction.STATUS_SKIPPED)
        assert(skipped.skipReason === Some(LogCompaction.SKIP_REASON_TARGET_EXISTS))
      }
    }
  }
}
