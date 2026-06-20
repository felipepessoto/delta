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

import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Verifies that [[org.apache.spark.sql.delta.hooks.LogCompactionHook]] honours the protocol
 * requirement that a log compaction file may only be produced for versions already published
 * (backfilled) in `_delta_log` (see PROTOCOL.md, "Maintenance Operations on Catalog-managed
 * Tables").
 *
 * On catalog-managed (catalog-owned / coordinated-commits) tables, committed versions can still be
 * staged under `_delta_log/_staged_commits`, and the snapshot tracks them via those staged files.
 * The hook therefore conservatively skips compaction for such tables (their maintenance is meant to
 * be coordinated by the catalog), so that it never produces a compaction over an unpublished
 * version. This suite locks in that behavior.
 */
class LogCompactionWithCatalogOwnedSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite {

  // Batched backfill so committed versions are staged when their post-commit hook runs.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)

  test("hook does not produce compactions on catalog-managed tables (commits may be staged)") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_LOG_COMPACTION_ENABLED.key -> "true",
      DeltaSQLConf.DELTALOG_LOG_COMPACTION_INTERVAL.key -> "5",
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        (0L to 20L).foreach { v =>
          spark.range(v, v + 1).write.format("delta").mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())

        val compactions = fs.listStatus(deltaLog.logPath)
          .filter(FileNames.isCompactedDeltaFile)
          .map(_.getPath.getName)
          .sorted

        // The hook must not produce any compaction file here: that would require compacting a
        // version that the snapshot still tracks as staged (unpublished).
        assert(compactions.isEmpty,
          s"expected no compaction files on a catalog-managed table, found: " +
            s"${compactions.mkString(", ")}")

        // The table still reads correctly.
        checkAnswer(spark.read.format("delta").load(path), spark.range(21).toDF())
      }
    }
  }
}
