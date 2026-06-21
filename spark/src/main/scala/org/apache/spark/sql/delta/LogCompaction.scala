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

import java.nio.file.FileAlreadyExistsException

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

  /** opType under which all log compaction telemetry is recorded. */
  private[delta] val OP_TYPE = "delta.logCompaction"

  // Possible values of `LogCompactionMetrics.status`.
  private[delta] val STATUS_COMPLETED = "completed"
  private[delta] val STATUS_SKIPPED = "skipped"

  // Possible values of `LogCompactionMetrics.skipReason` (only set when status is "skipped").
  private[delta] val SKIP_REASON_TARGET_EXISTS = "targetAlreadyExists"
  private[delta] val SKIP_REASON_CONCURRENT_WRITE = "concurrentlyCreated"

  /**
   * Creates a log compaction file for the table covering commits `[startVersion, endVersion]`
   * (both inclusive). The resulting file contains the reconciled actions for the range (following
   * the same rules as checkpoint creation) without `commitInfo`.
   *
   * The individual commit files in the range are read using the path resolution of the provided
   * `snapshot`, so this works for both regular tables and coordinated-commits / catalog-managed
   * tables (where recent commits may live under `_delta_log/_staged_commits`).
   *
   * This is a no-op if a compaction file for the exact `[startVersion, endVersion]` range already
   * exists (idempotent): the common case is short-circuited by an existence check that avoids
   * redundant reconciliation, and the write itself uses `overwrite = false` so a concurrent writer
   * that produced the same (deterministic) file cannot be clobbered.
   *
   * Each invocation emits a `delta.logCompaction.stats` telemetry event (see
   * [[LogCompactionMetrics]]) capturing the outcome (completed / skipped with a reason), duration,
   * number of commits and actions, and the resulting file size.
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
    val compactedFilePath =
      FileNames.compactedDeltaFile(deltaLog.logPath, startVersion, endVersion)
    val fs = deltaLog.logPath.getFileSystem(hadoopConf)
    val startTimeMs = System.currentTimeMillis()
    def elapsedMs: Long = System.currentTimeMillis() - startTimeMs

    // Idempotency fast path: skip the (potentially expensive) reconciliation and write if the
    // compaction file for this exact range already exists, e.g. because a concurrent writer
    // already produced it. This avoids recomputing the file in the common case; the write below
    // additionally closes the residual race window atomically via overwrite = false.
    if (fs.exists(compactedFilePath)) {
      logInfo(
        log"Skipping log compaction; file already exists " +
        log"${MDC(DeltaLogKeys.PATH, compactedFilePath)}")
      recordCompactionStats(deltaLog, LogCompactionMetrics(
        startVersion = startVersion,
        endVersion = endVersion,
        status = STATUS_SKIPPED,
        durationMs = elapsedMs,
        skipReason = Some(SKIP_REASON_TARGET_EXISTS)))
      return
    }

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

    // Count the reconciled actions as a side effect while the store consumes the iterator below.
    var numActions = 0L
    val actionsToWrite = logReplay.checkpoint.map { action =>
      numActions += 1
      action.json
    }

    // Write with overwrite = false so that, in the residual race where another writer produced the
    // same compaction file between the existence check above and this write, we don't clobber it.
    // The reconciled content for a given [startVersion, endVersion] range is deterministic, so an
    // already-present file is equivalent; a FileAlreadyExistsException is therefore treated as a
    // successful no-op rather than an error.
    try {
      deltaLog.store.write(
        path = compactedFilePath,
        actions = actionsToWrite,
        overwrite = false,
        hadoopConf = hadoopConf)
    } catch {
      case _: FileAlreadyExistsException =>
        logInfo(
          log"Skipping log compaction; file already exists " +
          log"${MDC(DeltaLogKeys.PATH, compactedFilePath)}")
        recordCompactionStats(deltaLog, LogCompactionMetrics(
          startVersion = startVersion,
          endVersion = endVersion,
          status = STATUS_SKIPPED,
          durationMs = elapsedMs,
          skipReason = Some(SKIP_REASON_CONCURRENT_WRITE)))
        return
    }

    recordCompactionStats(deltaLog, LogCompactionMetrics(
      startVersion = startVersion,
      endVersion = endVersion,
      status = STATUS_COMPLETED,
      durationMs = elapsedMs,
      numCommitsCompacted = endVersion - startVersion + 1,
      numActions = numActions,
      compactedFileSizeBytes = fs.getFileStatus(compactedFilePath).getLen))

    logInfo(
      log"Created log compaction file " +
      log"${MDC(DeltaLogKeys.PATH, compactedFilePath)} " +
      log"for versions [${MDC(DeltaLogKeys.START_VERSION, startVersion)}, " +
      log"${MDC(DeltaLogKeys.END_VERSION, endVersion)}]")
  }

  /** Emits a single log compaction telemetry event. */
  private def recordCompactionStats(deltaLog: DeltaLog, metrics: LogCompactionMetrics): Unit =
    recordDeltaEvent(deltaLog, opType = s"$OP_TYPE.stats", data = metrics)
}

/**
 * Telemetry for a single [[LogCompaction.compact]] invocation, emitted as the JSON blob of the
 * `delta.logCompaction.stats` event.
 *
 * @param startVersion The (inclusive) start version of the compaction range.
 * @param endVersion The (inclusive) end version of the compaction range.
 * @param status `completed` if a file was written, `skipped` if it already existed.
 * @param durationMs Wall-clock time spent in `compact`, in milliseconds.
 * @param skipReason When `status == skipped`, why it was skipped (see `SKIP_REASON_*`).
 * @param numCommitsCompacted Number of commit files reconciled into the compaction file.
 * @param numActions Number of actions written to the compaction file.
 * @param compactedFileSizeBytes Size of the written compaction file, in bytes.
 */
case class LogCompactionMetrics(
    startVersion: Long,
    endVersion: Long,
    status: String,
    durationMs: Long,
    skipReason: Option[String] = None,
    numCommitsCompacted: Long = 0L,
    numActions: Long = 0L,
    compactedFileSizeBytes: Long = 0L)
