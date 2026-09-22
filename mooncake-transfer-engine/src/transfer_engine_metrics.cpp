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

#include "transfer_engine_metrics.h"

#include <glog/logging.h>

#include <iomanip>
#include <sstream>
#include <utility>
#include <vector>

namespace mooncake {

TransferEngineMetrics& TransferEngineMetrics::instance() {
    static TransferEngineMetrics instance;
    return instance;
}

namespace {

// Latency buckets in microseconds, and size buckets in bytes: TENT's
// kLatencyBuckets / kSizeBuckets, so both engines bucket alike. Microseconds
// (rather than seconds) keep the integer histogram _sum meaningful for
// sub-millisecond RDMA transfers.
const std::vector<double> kLatencyBucketsUs{
    100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000};
const std::vector<double> kSizeBuckets{1024,     4096,      16384,     65536,
                                       262144,   1048576,   4194304,   16777216,
                                       67108864, 268435456, 1073741824};

std::string metricName(const std::string& direction, const char* suffix) {
    return "mooncake_te_" + direction + suffix;
}

// Help strings shared by the constructor and by resetForTesting(), which
// rebuilds the histograms.
std::string latencyHelp(const std::string& direction) {
    return "Latency distribution of " + direction +
           " transfers observed completed, submission to first observation, "
           "in microseconds";
}

std::string sizeHelp(const std::string& direction) {
    return "Size distribution of " + direction +
           " transfers observed completed, in bytes";
}

}  // namespace

TransferEngineMetrics::DirectionMetrics::DirectionMetrics(std::string dir)
    : direction(std::move(dir)),
      bytes_total(metricName(direction, "_bytes_total"),
                  "Total bytes transferred by " + direction +
                      " transfers observed completed"),
      requests_total(
          metricName(direction, "_requests_total"),
          "Total " + direction + " transfers observed in a terminal state"),
      failures_total(metricName(direction, "_failures_total"),
                     "Total " + direction +
                         " transfers observed failed, canceled or timed out"),
      latency_us(metricName(direction, "_latency_us"), latencyHelp(direction),
                 kLatencyBucketsUs),
      size_bytes(metricName(direction, "_size_bytes"), sizeHelp(direction),
                 kSizeBuckets) {
    // ylt leaves a counter out of /metrics until it is first touched
    // (counter.hpp: `if (value == 0 && !has_change_) return;`). inc(0) marks
    // it touched without changing it, so failures_total reads 0 while healthy
    // instead of first appearing at 1, which increase() would miss.
    bytes_total.inc(0);
    requests_total.inc(0);
    failures_total.inc(0);
}

TransferEngineMetrics::TransferEngineMetrics() {
    for (auto* dir : {&read_, &write_}) {
        exporter_.addCounter(&dir->bytes_total);
        exporter_.addCounter(&dir->requests_total);
        exporter_.addCounter(&dir->failures_total);
        exporter_.addHistogram(&dir->latency_us);
        exporter_.addHistogram(&dir->size_bytes);
    }
    exporter_.setSummaryProvider([this]() { return buildSummary(); });
}

void TransferEngineMetrics::initializeFromEnv() {
    auto config = metrics::ExporterConfig::fromEnv("MC_TE_METRIC");

    std::lock_guard<std::mutex> guard(init_mutex_);
    if (initialized_) {
        // A server was asked for but none is listening: the first engine's
        // bind failed (typically two ranks racing for one port at startup).
        // Retry for this engine rather than leaving the process without a
        // scrape endpoint for good; the exporter keeps collecting either way.
        if (config.http_port != 0 && exporter_.httpPort() == 0) {
            if (exporter_.start(config)) {
                initial_config_.http_host = config.http_host;
                initial_config_.http_port = config.http_port;
            }
        }
        // Otherwise the first engine's settings stand and this engine's are
        // dropped. That is intended -- one process serves one /metrics -- but
        // silently ignoring an explicit MC_TE_METRIC_HTTP_PORT is very hard to
        // diagnose from the outside, so say so once per extra engine.
        if (config.http_port != initial_config_.http_port ||
            config.http_host != initial_config_.http_host ||
            config.report_interval_seconds !=
                initial_config_.report_interval_seconds) {
            LOG(WARNING)
                << "Transfer Engine metrics are already exported by an earlier "
                   "engine in this process (host="
                << initial_config_.http_host
                << ", port=" << exporter_.httpPort() << ", report_interval="
                << initial_config_.report_interval_seconds
                << "s); ignoring this engine's host=" << config.http_host
                << ", port=" << config.http_port
                << ", report_interval=" << config.report_interval_seconds
                << "s. The exporter is process-wide and every engine already "
                   "records into it.";
        }
        return;
    }
    initial_config_ = config;
    initialized_ = true;

    // The periodic summary log line stays off by default: Classic TE already
    // prints its own throughput/latency line on MC_TE_METRIC_INTERVAL_SECONDS.
    // A false return means the HTTP bind failed; the exporter has already
    // logged it and keeps collecting, so there is nothing more to do here.
    exporter_.start(config);
}

// Every recorder is gated here rather than at its call sites, so that a call
// site is one line and cannot get the gate wrong. Before initializeFromEnv()
// (MC_TE_METRIC unset) each call is a single atomic load.
void TransferEngineMetrics::recordCompleted(Direction direction, size_t bytes,
                                            uint64_t latency_us) {
    if (!exporter_.isCollecting()) return;
    auto& dir = forDirection(direction);
    // inc(0) is a no-op, so a zero-byte completion needs no special case; it
    // is still a sample in size_bytes and latency_us.
    dir.requests_total.inc();
    dir.bytes_total.inc(static_cast<int64_t>(bytes));
    dir.size_bytes.observe(static_cast<int64_t>(bytes));
    dir.latency_us.observe(static_cast<int64_t>(latency_us));
}

void TransferEngineMetrics::recordFailed(Direction direction) {
    if (!exporter_.isCollecting()) return;
    auto& dir = forDirection(direction);
    dir.requests_total.inc();
    dir.failures_total.inc();
}

std::string TransferEngineMetrics::buildSummary() {
    auto formatBytes = [](double bytes) -> std::string {
        std::ostringstream s;
        s << std::fixed << std::setprecision(2);
        if (bytes >= 1e12)
            s << bytes / 1e12 << " TB";
        else if (bytes >= 1e9)
            s << bytes / 1e9 << " GB";
        else if (bytes >= 1e6)
            s << bytes / 1e6 << " MB";
        else if (bytes >= 1e3)
            s << bytes / 1e3 << " KB";
        else
            s << bytes << " B";
        return s.str();
    };

    std::ostringstream oss;
    auto append = [&](const char* label, DirectionMetrics& dir) {
        oss << label << ": " << formatBytes(dir.bytes_total.value()) << " ("
            << static_cast<uint64_t>(dir.requests_total.value()) << " reqs, "
            << static_cast<uint64_t>(dir.failures_total.value()) << " fails)";
    };
    append("Read", read_);
    oss << " | ";
    append("Write", write_);
    return oss.str();
}

void TransferEngineMetrics::DirectionMetrics::resetForTesting() {
    bytes_total.reset();
    requests_total.reset();
    failures_total.reset();
    // histogram_t has no reset(); rebuilding in place keeps the addresses the
    // exporter holds valid.
    latency_us.histogram =
        ylt::metric::histogram_t(metricName(direction, "_latency_us"),
                                 latencyHelp(direction), kLatencyBucketsUs);
    latency_us.sum.store(0, std::memory_order_relaxed);
    size_bytes.histogram =
        ylt::metric::histogram_t(metricName(direction, "_size_bytes"),
                                 sizeHelp(direction), kSizeBuckets);
    size_bytes.sum.store(0, std::memory_order_relaxed);
}

void TransferEngineMetrics::resetForTesting() {
    read_.resetForTesting();
    write_.resetForTesting();
}

}  // namespace mooncake
