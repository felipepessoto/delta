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

import org.apache.spark.sql.delta.actions.{Action, InMemoryLogReplay}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames}

import org.apache.spark.internal.MDC

/**
 * Utilities for creating log compaction files. A log compaction file aggregates the actions
 * from a range of commit files `[startVersion, endVersion]` into a single JSON file by following
 * the same action reconciliation rules as a checkpoint, but without full state reconstruction.
 *
 * Per the Delta protocol, log compaction files:
 *  - Reside in `_delta_log` and are named `<startVersion>.<endVersion>.compacted.json`.
 *  - Contain the reconciled actions for the commit range (one action per line), without
 *    `commitInfo` actions (these are stripped during reconciliation, same as checkpoints).
 *  - Are optional: writers may produce them and readers may consume them. They do not require any
 *    protocol or table-feature upgrade; readers that do not understand them simply ignore them.
 *  - Replace the corresponding individual commit files during snapshot construction on the read
 *    path, speeding it up while keeping the checkpoint interval high.
 */
object LogCompaction extends DeltaLogging {

  /**
   * Creates a log compaction file for the table covering commits `[startVersion, endVersion]`
   * (both inclusive). The resulting file contains the reconciled actions for the range (following
   * the same rules as checkpoint creation) without `commitInfo`.
   *
   * The individual commit files in the range are read using the path resolution of the provided
   * `snapshot`, so this works for both regular tables and coordinated-commits / catalog-managed
   * tables (where recent commits may live under `_delta_log/_staged_commits`).
   *
   * @param deltaLog The [[DeltaLog]] instance for the table.
   * @param snapshot A snapshot at or after `endVersion`, used to resolve commit file paths.
   * @param startVersion The start version of the compaction range (inclusive).
   * @param endVersion The end version of the compaction range (inclusive).
   */
  def compact(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      startVersion: Long,
      endVersion: Long): Unit = {
    require(
      endVersion > startVersion,
      s"endVersion ($endVersion) must be greater than startVersion ($startVersion)")

    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val fileProvider = DeltaCommitFileProvider(snapshot)
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None,
      minSetTransactionRetentionTimestamp = None)

    (startVersion to endVersion).foreach { version =>
      val file = fileProvider.deltaFile(version)
      // `readAsIterator` returns a ClosableIterator backed by an open input stream. Keep a
      // reference to it (rather than chaining `.map`, which discards the close handle) and close
      // it explicitly so the stream is released even if reconciliation is interrupted mid-file.
      val actions = deltaLog.store.readAsIterator(file, hadoopConf)
      try {
        logReplay.append(version, actions.map(Action.fromJson))
      } finally {
        actions.close()
      }
    }

    val compactedFilePath =
      FileNames.compactedDeltaFile(deltaLog.logPath, startVersion, endVersion)
    deltaLog.store.write(
      path = compactedFilePath,
      actions = logReplay.checkpoint.map(_.json),
      overwrite = true,
      hadoopConf = hadoopConf)

    logInfo(
      log"Created log compaction file " +
      log"${MDC(DeltaLogKeys.PATH, compactedFilePath)} " +
      log"for versions [${MDC(DeltaLogKeys.START_VERSION, startVersion)}, " +
      log"${MDC(DeltaLogKeys.END_VERSION, endVersion)}]")
  }
}
