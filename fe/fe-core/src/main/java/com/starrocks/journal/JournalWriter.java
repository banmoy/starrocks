// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.Util;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * An independent thread to write journals by batch asynchronously.
 * Each thread that needs to write a log can put the log in a blocking queue, while JournalWriter constantly gets as
 * many logs as possible from the queue and write them all in one batch.
 * After committing, JournalWriter will notify the caller thread for consistency.
 */
public class JournalWriter {
    public static final Logger LOG = LogManager.getLogger(JournalWriter.class);
    // other threads can put log to this queue by calling Editlog.logEdit()
    private BlockingQueue<JournalTask> journalQueue;
    private Journal journal;

    // used for checking if edit log need to roll
    protected long rollJournalCounter = 0;
    // increment journal id
    // this is the persisted journal id
    protected long nextVisibleJournalId = -1;

    // belows are variables that will reset every batch
    // store journal tasks of this batch
    protected PendingCommitTasks pendingCommitTasks;

    /**
     * If this flag is set true, we will roll journal,
     * i.e. create a new database in BDB immediately after
     * current journal batch has been written.
     */
    private boolean forceRollJournal;

    /** Last timestamp in millisecond to log the commit triggered by delay. */
    private long lastLogTimeForDelayTriggeredCommit = -1;

    public JournalWriter(Journal journal, BlockingQueue<JournalTask> journalQueue) {
        this.journal = journal;
        this.journalQueue = journalQueue;
        this.pendingCommitTasks = new PendingCommitTasks();
    }

    /**
     * reset journal id & roll journal as a start
     */
    public void init(long maxJournalId) throws JournalException {
        this.nextVisibleJournalId = maxJournalId + 1;
        this.journal.rollJournal(this.nextVisibleJournalId);
    }

    public void startDaemon() {
        // ensure init() is called.
        assert (nextVisibleJournalId > 0);
        Daemon d = new Daemon("JournalWriter", 0L) {
            @Override
            protected void runOneCycle() {
                try {
                    writeOneBatch();
                } catch (InterruptedException e) {
                    String msg = "got interrupted exception when trying to write one batch, will exit now.";
                    LOG.error(msg, e);
                    // TODO we should exit gracefully on InterruptedException
                    Util.stdoutWithTime(msg);
                    System.exit(-1);
                }
            }
        };
        d.start();
    }

    protected void writeOneBatch() throws InterruptedException {
        // waiting if necessary until an element becomes available
        JournalTask journalTask = journalQueue.take();
        long nextJournalId = nextVisibleJournalId;
        pendingCommitTasks.reset();

        try {
            this.journal.batchWriteBegin();

            while (true) {
                for (DataOutputBuffer buffer : journalTask.getBuffer()) {
                    journal.batchWriteAppend(nextJournalId, buffer);
                    nextJournalId += 1;
                }
                pendingCommitTasks.addJournalTask(journalTask);

                if (shouldCommitNow()) {
                    break;
                }

                journalTask = journalQueue.take();
            }
        } catch (JournalException e) {
            // abort current task
            LOG.warn("failed to write batch, will abort current journal {} and commit", journalTask, e);
            abortJournalTask(journalTask, e.getMessage());
        } finally {
            try {
                // commit
                journal.batchWriteCommit();
                LOG.debug("batch write commit success, from {} - {}", nextVisibleJournalId, nextJournalId);
                nextVisibleJournalId = nextJournalId;
                markCurrentBatchSucceed();
            } catch (JournalException e) {
                // abort
                LOG.warn("failed to commit batch, will abort current {} journals.", pendingCommitTasks.getNumJournals(), e);
                try {
                    journal.batchWriteAbort();
                } catch (JournalException e2) {
                    LOG.warn("failed to abort batch, will ignore and continue.", e);
                }
                abortCurrentBatch(e.getMessage());
            }
        }

        rollJournalAfterBatch();

        updateBatchMetrics();
    }

    private void markCurrentBatchSucceed() {
        for (JournalTask t : pendingCommitTasks.getJournalTasks()) {
            t.markSucceed();
        }
    }

    private void abortCurrentBatch(String errMsg) {
        for (JournalTask t : pendingCommitTasks.getJournalTasks()) {
            abortJournalTask(t, errMsg);
        }
    }

    /**
     * We should notify the caller to rollback or report error on abort, like this.
     *
     * task.markAbort();
     *
     * But now we have to exit for historical reason.
     * Note that if we exit here, the final clause(commit current batch) will not be executed.
     */
    protected void abortJournalTask(JournalTask task, String msg) {
        LOG.error(msg);
        Util.stdoutWithTime(msg);
        System.exit(-1);
    }

    private boolean shouldCommitNow() {
        // 1. check if is an emergency journal
        if (pendingCommitTasks.getBetterCommitTimeInNano() > 0) {
            long betterCommitTime = pendingCommitTasks.getBetterCommitTimeInNano();
            long delayNanos = System.nanoTime() - betterCommitTime;
            if (delayNanos >= 0) {
                long logTime = System.currentTimeMillis();
                // avoid logging too many messages if triggered frequently
                if (lastLogTimeForDelayTriggeredCommit + 500 < logTime) {
                    lastLogTimeForDelayTriggeredCommit = logTime;
                    LOG.warn("journal expect commit before {} is delayed {} nanos, will commit now",
                            betterCommitTime, delayNanos);
                }
                return true;
            }
        }

        // 2. check uncommitted journal by count
        if (pendingCommitTasks.getNumJournals() >= Config.metadata_journal_max_batch_cnt) {
            LOG.warn("uncommitted journal {} >= {}, will commit now",
                    pendingCommitTasks.getNumJournals(), Config.metadata_journal_max_batch_cnt);
            return true;
        }

        // 3. check uncommitted journals by size
        if (pendingCommitTasks.getEstimatedBytes() >= Config.metadata_journal_max_batch_size_mb * 1024 * 1024) {
            LOG.warn("uncommitted estimated bytes {} >= {}MB, will commit now",
                    pendingCommitTasks.getEstimatedBytes(), Config.metadata_journal_max_batch_size_mb);
            return true;
        }

        // 4. no more journal in queue
        return journalQueue.peek() == null;
    }

    /**
     * update all metrics after batch write
     */
    private void updateBatchMetrics() {
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_WRITE.increase((long) pendingCommitTasks.getNumJournals());
            MetricRepo.HISTO_JOURNAL_WRITE_LATENCY.update((System.nanoTime() - pendingCommitTasks.getStartTimeInNano()) / 1000000);
            MetricRepo.HISTO_JOURNAL_WRITE_BATCH.update(pendingCommitTasks.getNumJournals());
            MetricRepo.HISTO_JOURNAL_WRITE_BYTES.update(pendingCommitTasks.getEstimatedBytes());
            MetricRepo.GAUGE_STACKED_JOURNAL_NUM.setValue((long) journalQueue.size());
            MetricRepo.COUNTER_EDIT_LOG_SIZE_BYTES.increase(pendingCommitTasks.getEstimatedBytes());
        }
        if (journalQueue.size() > Config.metadata_journal_max_batch_cnt) {
            LOG.warn("journal has piled up: {} in queue after consume", journalQueue.size());
        }
    }

    public void setForceRollJournal() {
        forceRollJournal = true;
    }

    private boolean needForceRollJournal() {
        if (forceRollJournal) {
            // Reset flag, alter system create image only trigger new image once
            forceRollJournal = false;
            return true;
        }

        return false;
    }

    private void rollJournalAfterBatch() {
        rollJournalCounter += pendingCommitTasks.getNumJournals();
        if (rollJournalCounter >= Config.edit_log_roll_num || needForceRollJournal()) {
            try {
                journal.rollJournal(nextVisibleJournalId);
            } catch (JournalException e) {
                String msg = String.format("failed to roll journal %d, will exit", nextVisibleJournalId);
                LOG.error(msg, e);
                Util.stdoutWithTime(msg);
                // TODO exit gracefully
                System.exit(-1);
            }
            String reason;
            if (rollJournalCounter >= Config.edit_log_roll_num) {
                reason = String.format("rollEditCounter {} >= edit_log_roll_num {}",
                        rollJournalCounter, Config.edit_log_roll_num);
            } else {
                reason = "triggering a new checkpoint manually";
            }
            LOG.info("edit log rolled because {}", reason);
            rollJournalCounter = 0;
        }
    }

    private static class PendingCommitTasks {

        private final List<JournalTask> journalTasks;
        private long startTimeInNano;
        private int numJournals;
        private long estimatedBytes;
        private long betterCommitTimeInNano;

        public PendingCommitTasks() {
            this.journalTasks = new ArrayList<>();
        }

        public void reset() {
            journalTasks.clear();
            startTimeInNano = System.nanoTime();
            numJournals = 0;
            estimatedBytes = 0L;
            betterCommitTimeInNano = -1;
        }

        public void addJournalTask(JournalTask task) {
            journalTasks.add(task);
            numJournals += task.numJournals();
            estimatedBytes += task.estimatedSizeByte();
            long taskCommitTime = task.getBetterCommitBeforeTimeInNano();
            if (taskCommitTime != -1) {
                betterCommitTimeInNano = betterCommitTimeInNano == -1 ? taskCommitTime : Math.min(betterCommitTimeInNano, taskCommitTime);
            }
        }

        public long getStartTimeInNano() {
            return startTimeInNano;
        }

        public int getNumJournals() {
            return numJournals;
        }

        public long getEstimatedBytes() {
            return estimatedBytes;
        }

        public long getBetterCommitTimeInNano() {
            return betterCommitTimeInNano;
        }

        public List<JournalTask> getJournalTasks() {
            return Collections.unmodifiableList(journalTasks);
        }
    }
}
