// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine_impl.h"
#include "transfer_engine_metrics.h"
#include "transport/transport.h"

namespace mooncake {

// Reaches into TransferEngineImpl / MultiTransport internals to build batches
// and tasks without RDMA or TCP plumbing, so the metrics recording can be
// tested hermetically. Both classes befriend this name.
class TransferEngineImplTestPeer {
   public:
    static void installTransport(TransferEngineImpl& engine,
                                 std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.clear();
        engine.multi_transports_->transport_map_.emplace("fake",
                                                         std::move(transport));
    }

    static BatchID allocateBatch(TransferEngineImpl& engine, size_t size) {
        return engine.multi_transports_->allocateBatchID(size);
    }

    // Append a task, stamp it the way submitTransfer() does before posting to
    // a transport, and give it the slice a transport that accepted it would.
    static void addSubmittedTask(BatchID batch_id, Transport* transport,
                                 const Transport::TransferRequest& request) {
        auto& batch = Transport::toBatchDesc(batch_id);
        batch.task_list.emplace_back();
        auto& task = batch.task_list.back();
        task.batch_id = batch_id;
        task.transport_ = transport;
        task.request = &request;
        MultiTransport::markTaskSubmitted(task, request);
        task.slice_count = 1;
    }

    // Finish every task the way a completion callback would: counters
    // updated, no status poll involved.
    static void markAllFinished(BatchID batch_id, size_t bytes_per_task = 0) {
        auto& batch = Transport::toBatchDesc(batch_id);
        for (auto& task : batch.task_list) {
            task.transferred_bytes = bytes_per_task;
            task.is_finished = true;
        }
    }

    static Status freeBatch(TransferEngineImpl& engine, BatchID batch_id) {
        return engine.multi_transports_->freeBatchID(batch_id);
    }

    static Status submitScatter(TransferEngineImpl& engine,
                                const std::vector<TransferRequest>& entries,
                                MultiTransport::ScatterSubmission& submission) {
        return engine.multi_transports_->submitScatter(entries, submission);
    }
};

namespace {

// Transport that reports a fixed terminal status for every task, or one
// status per task via setTaskStatuses().
class FakeTransport : public Transport {
   public:
    FakeTransport(TransferStatusEnum status, size_t transferred_bytes)
        : status_(status), transferred_bytes_(transferred_bytes) {}

    // Status returned by submitTransferTask(), i.e. whether this transport
    // accepts the tasks MultiTransport posts to it. A refusing transport still
    // posts the first `tasks_posted` tasks of a submission, the way
    // RdmaTransport can fail partway through a batch.
    void setSubmitStatus(Status status, size_t tasks_posted = 0) {
        submit_status_ = std::move(status);
        tasks_posted_before_refusal_ = tasks_posted;
    }

    // Report TIMEOUT for the next `polls` status queries, whatever the task,
    // before falling back to the configured statuses.
    void setTimeoutPolls(int polls) { timeout_polls_ = polls; }

    // Per-task statuses, overriding the constructor's single status. A task
    // with a terminal status also gets its is_finished flag set, the way every
    // real transport's getTransferStatus() does.
    void setTaskStatuses(std::vector<TransferStatusEnum> statuses) {
        task_statuses_ = std::move(statuses);
    }

    bool supportsGroupedScatter() const override { return true; }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    // Every real transport counts a slice per task it posts; a task refused
    // before that keeps slice_count == 0 and reads as COMPLETED with 0 bytes
    // (success + failed == slice_count) from its getTransferStatus().
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        size_t posted =
            submit_status_.ok()
                ? tasks.size()
                : std::min(tasks_posted_before_refusal_, tasks.size());
        for (size_t i = 0; i < posted; ++i)
            __sync_fetch_and_add(&tasks[i]->slice_count, 1);
        return submit_status_;
    }

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override {
        if (timeout_polls_ > 0) {
            // Like MultiTransport's slice-age check: the task is still in
            // flight, so is_finished stays false.
            --timeout_polls_;
            status.s = TransferStatusEnum::TIMEOUT;
            status.transferred_bytes = 0;
            return Status::OK();
        }
        status.s = status_;
        status.transferred_bytes = transferred_bytes_;
        if (task_id < task_statuses_.size()) {
            status.s = task_statuses_[task_id];
            if (status.s != TransferStatusEnum::COMPLETED)
                status.transferred_bytes = 0;
        }
        if (status.s != TransferStatusEnum::WAITING &&
            status.s != TransferStatusEnum::TIMEOUT)
            toBatchDesc(batch_id).task_list[task_id].is_finished = true;
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "fake"; }

    TransferStatusEnum status_;
    size_t transferred_bytes_;
    std::vector<TransferStatusEnum> task_statuses_;
    Status submit_status_ = Status::OK();
    size_t tasks_posted_before_refusal_ = 0;
    int timeout_polls_ = 0;
};

using Direction = TransferEngineMetrics::Direction;

// Test-side sugar over the series the exporter holds.
TransferEngineMetrics::DirectionMetrics& seriesFor(Direction direction) {
    return TransferEngineMetrics::instance().forDirection(direction);
}
double bytesTotal(Direction d) { return seriesFor(d).bytes_total.value(); }
double requestsTotal(Direction d) {
    return seriesFor(d).requests_total.value();
}
double failuresTotal(Direction d) {
    return seriesFor(d).failures_total.value();
}
int64_t latencyCount(Direction d) { return seriesFor(d).latency_us.count(); }
int64_t sizeCount(Direction d) { return seriesFor(d).size_bytes.count(); }

Transport::TransferRequest makeRequest(
    Transport::TransferRequest::OpCode opcode, size_t length) {
    Transport::TransferRequest request{};
    request.opcode = opcode;
    request.source = nullptr;
    request.target_id = 0;
    request.target_offset = 0;
    request.length = length;
    return request;
}

class MetricsTestBase : public ::testing::Test {
   protected:
    void SetUp() override {
        // Collect without binding a socket or starting a report thread,
        // whatever the developer's environment happens to hold.
        unsetenv("MC_TE_METRIC_HTTP_PORT");
        unsetenv("MC_TE_METRIC_REPORT_INTERVAL_SECONDS");
        auto& metrics = TransferEngineMetrics::instance();
        metrics.initializeFromEnv();
        metrics.resetForTesting();
    }

    TransferEngineMetrics& metrics() {
        return TransferEngineMetrics::instance();
    }
};

// --- Direct record API -----------------------------------------------------

TEST_F(MetricsTestBase, ReadAndWriteAreAccountedSeparately) {
    auto& m = metrics();
    m.recordCompleted(Direction::Read, 4096, 2000);
    m.recordFailed(Direction::Write);

    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(bytesTotal(Direction::Read), 4096);
    EXPECT_EQ(failuresTotal(Direction::Read), 0);
    EXPECT_EQ(latencyCount(Direction::Read), 1);
    EXPECT_EQ(sizeCount(Direction::Read), 1);

    EXPECT_EQ(requestsTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 0);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(latencyCount(Direction::Write), 0);
}

// A completion measured as 0us must still land in the histogram, otherwise the
// sample count drifts below the number of completed transfers for fast local
// copies.
TEST_F(MetricsTestBase, ZeroLatencyCompletionIsObserved) {
    metrics().recordCompleted(Direction::Read, 128, 0);
    EXPECT_EQ(latencyCount(Direction::Read), 1);
}

// Before initializeFromEnv() (MC_TE_METRIC unset) every recorder is a no-op,
// so the transports can call them unconditionally.
TEST_F(MetricsTestBase, RecordersAreNoOpsWhileNotCollecting) {
    auto& m = metrics();
    m.shutdown();
    ASSERT_FALSE(m.isRecording());
    m.recordCompleted(Direction::Read, 4096, 10);
    m.recordFailed(Direction::Write);
    m.initializeFromEnv();

    EXPECT_EQ(requestsTotal(Direction::Read), 0);
    EXPECT_EQ(requestsTotal(Direction::Write), 0);
}

TEST_F(MetricsTestBase, PrometheusTextCarriesPrefixedSeries) {
    auto& m = metrics();
    m.recordCompleted(Direction::Read, 1048576, 12345);

    const std::string text = m.prometheusText();
    EXPECT_NE(text.find("mooncake_te_read_bytes_total"), std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_requests_total"), std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_bucket"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_sum"), std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_count"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_size_bytes_bucket"),
              std::string::npos);
    // The latency sum is exported in microseconds, so a 12.345ms transfer must
    // not be rounded away.
    EXPECT_EQ(text.find("mooncake_te_read_latency_us_sum 0"),
              std::string::npos);
    // Untouched counters must still be exported as 0, so that a failure-ratio
    // query has a series while healthy and increase() sees the first failure.
    EXPECT_NE(text.find("mooncake_te_read_failures_total 0"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_write_requests_total 0"),
              std::string::npos);
}

TEST_F(MetricsTestBase, JsonTextCarriesHistogramSums) {
    auto& m = metrics();
    m.recordCompleted(Direction::Read, 1048576, 2000000);
    m.recordCompleted(Direction::Read, 1024, 500000);

    // Same shape as TENT's /metrics/json: a histogram is an object with count,
    // sum and buckets, so a consumer can compute exact averages.
    const std::string text = m.jsonText();
    EXPECT_NE(text.find("\"mooncake_te_read_latency_us\":{\"count\":2,"
                        "\"sum\":2500000,\"buckets\":{"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("\"mooncake_te_read_size_bytes\":{\"count\":2,"
                        "\"sum\":1049600,\"buckets\":{"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("\"mooncake_te_write_latency_us\":{\"count\":0,"
                        "\"sum\":0,\"buckets\":{"),
              std::string::npos)
        << text;
}

// --- End to end through TransferEngineImpl ---------------------------------

class EngineMetricsTest : public MetricsTestBase {
   protected:
    void SetUp() override {
        setenv("MC_TE_METRIC", "1", /*overwrite=*/1);
        MetricsTestBase::SetUp();
    }

    // Make requests targeting kFakeSegmentId resolve to a segment served by
    // the "fake" protocol, so MultiTransport::selectTransport() routes them to
    // the installed FakeTransport.
    static constexpr SegmentID kFakeSegmentId = 42;
    void installFakeSegment(TransferEngineImpl& engine) {
        const std::string name = "fake-segment";
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = name;
        desc->protocol = "fake";
        engine.getMetadata()->addLocalSegment(kFakeSegmentId, name,
                                              std::move(desc));
    }

    // Builds an engine with `transport` installed and a batch holding
    // `task_count` submitted tasks of the given direction.
    void buildBatch(TransferEngineImpl& engine, const char* listen_addr,
                    std::shared_ptr<Transport> transport,
                    Transport::TransferRequest::OpCode opcode,
                    size_t task_count) {
        ASSERT_EQ(engine.init(P2PHANDSHAKE, listen_addr), 0);
        TransferEngineImplTestPeer::installTransport(engine, transport);
        request_ = makeRequest(opcode, 8192);
        batch_ = TransferEngineImplTestPeer::allocateBatch(engine, task_count);
        for (size_t i = 0; i < task_count; ++i) {
            TransferEngineImplTestPeer::addSubmittedTask(
                batch_, transport.get(), request_);
        }
    }

    // Builds an engine that routes kFakeSegmentId to `transport` and returns
    // `count` requests of `opcode` addressed to it, for the real submit path.
    std::vector<Transport::TransferRequest> buildSubmitEntries(
        TransferEngineImpl& engine, const char* listen_addr,
        std::shared_ptr<Transport> transport,
        Transport::TransferRequest::OpCode opcode, size_t count) {
        EXPECT_EQ(engine.init(P2PHANDSHAKE, listen_addr), 0);
        TransferEngineImplTestPeer::installTransport(engine, transport);
        installFakeSegment(engine);
        request_ = makeRequest(opcode, 8192);
        request_.target_id = kFakeSegmentId;
        batch_ = TransferEngineImplTestPeer::allocateBatch(engine, count);
        return std::vector<Transport::TransferRequest>(count, request_);
    }

    void TearDown() override {
        if (batch_) {
            TransferEngineImplTestPeer::markAllFinished(batch_);
        }
    }

    Transport::TransferRequest request_{};
    BatchID batch_ = 0;
};

// Polling per-task status repeatedly must record the completion exactly once.
TEST_F(EngineMetricsTest, RepeatedTaskPollRecordsOnce) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    buildBatch(engine, "127.0.0.1:12399", transport,
               Transport::TransferRequest::READ, 1);

    ASSERT_EQ(requestsTotal(Direction::Read), 0);

    TransferStatus status;
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
        EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    }

    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(bytesTotal(Direction::Read), 8192);
    EXPECT_EQ(latencyCount(Direction::Read), 1);
    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

// The batch status API is the path Python's batch_transfer_sync uses. It
// reaches tasks through MultiTransport::getTransferStatus without going through
// TransferEngineImpl::getTransferStatus, so recording has to happen there or
// no latency is ever observed. Polling a batch and then its tasks, or the
// other way round, must still record each task once.
TEST_F(EngineMetricsTest, BatchAndTaskPollsRecordEachTaskOnce) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    buildBatch(engine, "127.0.0.1:12398", transport,
               Transport::TransferRequest::WRITE, 2);

    ASSERT_EQ(requestsTotal(Direction::Write), 0);

    TransferStatus status;
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);

    EXPECT_EQ(requestsTotal(Direction::Write), 2);
    EXPECT_EQ(bytesTotal(Direction::Write), 16384);
    EXPECT_EQ(latencyCount(Direction::Write), 2);
    EXPECT_EQ(sizeCount(Direction::Write), 2);

    // Polling the finished batch again, or its tasks one by one (which also
    // runs the legacy log path that clears start_time), must not double count.
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_, status).ok());
    ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
    ASSERT_TRUE(engine.getTransferStatus(batch_, 1, status).ok());
    ASSERT_TRUE(engine.getTransferStatus(batch_, 1, status).ok());
    EXPECT_EQ(requestsTotal(Direction::Write), 2);
    EXPECT_EQ(latencyCount(Direction::Write), 2);

    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

TEST_F(EngineMetricsTest, FailedTaskCountsAsFailureOnce) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::FAILED, 0);
    buildBatch(engine, "127.0.0.1:12397", transport,
               Transport::TransferRequest::READ, 1);

    TransferStatus status;
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
        EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    }

    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(failuresTotal(Direction::Read), 1);
    EXPECT_EQ(bytesTotal(Direction::Read), 0);
    EXPECT_EQ(latencyCount(Direction::Read), 0);

    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

// In Classic, TIMEOUT is a per-poll signal that a task's slices have been in
// flight past MC_SLICE_TIMEOUT, not an outcome: the caller keeps polling and
// the task still completes or fails. Recording it as a failure would also
// drop the completion that follows behind the once-flag.
TEST_F(EngineMetricsTest, TimeoutPollIsNotTerminal) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    transport->setTimeoutPolls(2);
    buildBatch(engine, "127.0.0.1:12390", transport,
               Transport::TransferRequest::READ, 1);

    TransferStatus status;
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
        EXPECT_EQ(status.s, Transport::TransferStatusEnum::TIMEOUT);
    }
    EXPECT_EQ(requestsTotal(Direction::Read), 0);
    EXPECT_EQ(failuresTotal(Direction::Read), 0);

    ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(failuresTotal(Direction::Read), 0);
    EXPECT_EQ(bytesTotal(Direction::Read), 8192);
    EXPECT_EQ(latencyCount(Direction::Read), 1);

    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

// The batch API turns a TIMEOUT task into a final FAILED for the whole batch,
// and callers treat that as the end of the batch. That task is recorded as a
// failure by the batch poll itself, and the once-flag keeps a later per-task
// poll that sees it complete from recording it a second time.
TEST_F(EngineMetricsTest, BatchPollRecordsTimeoutAsFailure) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    transport->setTimeoutPolls(1);
    buildBatch(engine, "127.0.0.1:12389", transport,
               Transport::TransferRequest::READ, 1);

    TransferStatus status;
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(failuresTotal(Direction::Read), 1);
    EXPECT_EQ(bytesTotal(Direction::Read), 0);
    EXPECT_EQ(latencyCount(Direction::Read), 0);

    ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(failuresTotal(Direction::Read), 1);
    EXPECT_EQ(bytesTotal(Direction::Read), 0);
    EXPECT_EQ(latencyCount(Direction::Read), 0);

    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

// A batch poll stops at the first failed task, so a sibling that is still in
// flight is not observed by that poll. It is recorded when the caller polls
// it to completion, and not otherwise -- the contract is observation-based.
TEST_F(EngineMetricsTest, BatchPollRecordsOnlyWhatItObserves) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 4096);
    transport->setTaskStatuses({Transport::TransferStatusEnum::FAILED,
                                Transport::TransferStatusEnum::COMPLETED});
    buildBatch(engine, "127.0.0.1:12393", transport,
               Transport::TransferRequest::WRITE, 2);

    TransferStatus status;
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(requestsTotal(Direction::Write), 1);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 0);

    // The caller goes on to poll the sibling: recorded as a completion.
    ASSERT_TRUE(engine.getTransferStatus(batch_, 1, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(requestsTotal(Direction::Write), 2);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 4096);

    EXPECT_TRUE(TransferEngineImplTestPeer::freeBatch(engine, batch_).ok());
    batch_ = 0;
}

// Tasks that finish without any status poll observing them are not recorded:
// there is no completion hook and no settlement at free.
TEST_F(EngineMetricsTest, UnpolledTasksAreNotRecorded) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 4096);
    buildBatch(engine, "127.0.0.1:12394", transport,
               Transport::TransferRequest::READ, 2);

    TransferEngineImplTestPeer::markAllFinished(batch_, 4096);
    EXPECT_TRUE(TransferEngineImplTestPeer::freeBatch(engine, batch_).ok());
    batch_ = 0;

    EXPECT_EQ(requestsTotal(Direction::Read), 0);
    EXPECT_EQ(bytesTotal(Direction::Read), 0);
    EXPECT_EQ(latencyCount(Direction::Read), 0);
}

// A transport that refuses the whole submission creates no slices, and the
// caller sees the error from submitTransfer() itself. Its tasks are stamped
// (the stamp precedes the post) and a real transport reports them as
// COMPLETED with 0 bytes, so a caller that polls anyway must not turn them
// into recorded transfers.
TEST_F(EngineMetricsTest, RefusedSubmitRecordsNothing) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 0);
    transport->setSubmitStatus(Status::InvalidArgument("refused"));
    auto entries = buildSubmitEntries(engine, "127.0.0.1:12395", transport,
                                      Transport::TransferRequest::WRITE, 3);
    EXPECT_FALSE(engine.submitTransfer(batch_, entries).ok());

    EXPECT_EQ(requestsTotal(Direction::Write), 0);
    EXPECT_EQ(failuresTotal(Direction::Write), 0);

    TransferStatus status;
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    for (size_t i = 0; i < entries.size(); ++i)
        ASSERT_TRUE(engine.getTransferStatus(batch_, i, status).ok());

    EXPECT_EQ(requestsTotal(Direction::Write), 0);
    EXPECT_EQ(failuresTotal(Direction::Write), 0);
    EXPECT_EQ(latencyCount(Direction::Write), 0);
    EXPECT_EQ(sizeCount(Direction::Write), 0);

    TransferEngineImplTestPeer::markAllFinished(batch_);
    EXPECT_TRUE(TransferEngineImplTestPeer::freeBatch(engine, batch_).ok());
    batch_ = 0;
}

// RdmaTransport can post the first tasks of a submission and then fail the
// rest. The posted task, once the caller polls it, is recorded with its real
// outcome and a latency measured from before the post.
TEST_F(EngineMetricsTest, PartialSubmitFailureStillRecordsPolledTasks) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    transport->setSubmitStatus(Status::InvalidArgument("mid-batch"),
                               /*tasks_posted=*/1);
    transport->setTaskStatuses({Transport::TransferStatusEnum::COMPLETED,
                                Transport::TransferStatusEnum::WAITING});
    auto entries = buildSubmitEntries(engine, "127.0.0.1:12392", transport,
                                      Transport::TransferRequest::WRITE, 2);
    EXPECT_FALSE(engine.submitTransfer(batch_, entries).ok());
    EXPECT_EQ(requestsTotal(Direction::Write), 0);

    TransferStatus status;
    ASSERT_TRUE(engine.getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(engine.getTransferStatus(batch_, 1, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::WAITING);

    EXPECT_EQ(requestsTotal(Direction::Write), 1);
    EXPECT_EQ(failuresTotal(Direction::Write), 0);
    EXPECT_EQ(bytesTotal(Direction::Write), 8192);
    EXPECT_EQ(latencyCount(Direction::Write), 1);

    TransferEngineImplTestPeer::markAllFinished(batch_);
    EXPECT_TRUE(TransferEngineImplTestPeer::freeBatch(engine, batch_).ok());
    batch_ = 0;
}

// A grouped scatter task is recorded once, under one opcode. A group that
// spans a read and a write would book every byte to the first direction, so
// grouping has to stop where the opcode changes.
TEST_F(EngineMetricsTest, MixedDirectionScatterIsSplitPerDirection) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12391"), 0);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    TransferEngineImplTestPeer::installTransport(engine, transport);
    installFakeSegment(engine);

    auto read = makeRequest(Transport::TransferRequest::READ, 4096);
    auto write = makeRequest(Transport::TransferRequest::WRITE, 8192);
    read.target_id = write.target_id = kFakeSegmentId;
    read.task_group_id = write.task_group_id = 1;
    std::vector<Transport::TransferRequest> entries{read, read, write};

    MultiTransport::ScatterSubmission submission;
    ASSERT_TRUE(
        TransferEngineImplTestPeer::submitScatter(engine, entries, submission)
            .ok());
    batch_ = submission.batch_id;
    ASSERT_EQ(submission.task_sizes, (std::vector<size_t>{2, 1}));

    TransferStatus status;
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(requestsTotal(Direction::Read), 1);
    EXPECT_EQ(bytesTotal(Direction::Read), 8192);
    EXPECT_EQ(requestsTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 8192);

    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

// Two threads polling the same terminal task race on the recorded flag; the
// task must be recorded exactly once.
TEST_F(EngineMetricsTest, ConcurrentPollsRecordOnce) {
    TransferEngineImpl engine(false);
    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 4096);
    constexpr size_t kTasks = 64;
    buildBatch(engine, "127.0.0.1:12396", transport,
               Transport::TransferRequest::READ, kTasks);

    std::vector<std::thread> pollers;
    for (int t = 0; t < 4; ++t) {
        pollers.emplace_back([&] {
            TransferStatus status;
            for (size_t i = 0; i < kTasks; ++i)
                engine.getTransferStatus(batch_, i, status);
        });
    }
    for (auto& p : pollers) p.join();

    EXPECT_EQ(requestsTotal(Direction::Read), kTasks);
    EXPECT_EQ(bytesTotal(Direction::Read), kTasks * 4096);
    EXPECT_EQ(latencyCount(Direction::Read), kTasks);

    TransferEngineImplTestPeer::freeBatch(engine, batch_);
    batch_ = 0;
}

}  // namespace
}  // namespace mooncake
