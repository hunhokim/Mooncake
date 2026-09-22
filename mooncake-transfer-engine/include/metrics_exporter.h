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

#ifndef MOONCAKE_METRICS_EXPORTER_H
#define MOONCAKE_METRICS_EXPORTER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Metrics scaffolding for the Classic Transfer Engine: a metric registry, the
// /metrics, /metrics/summary, /metrics/json and /health endpoints, the periodic
// report thread and the collecting switch.
//
// An owner declares its own metrics, registers them once, and calls start().
// Nothing here knows about transfers, engines or transports, so the class can
// move to mooncake-common if a second component adopts it; today the only owner
// is TransferEngineMetrics.
//
// The endpoint paths and output formats are the ones TENT serves
// (mooncake-transfer-engine/tent/src/metrics/tent_metrics.cpp), so a scrape
// config or dashboard written for one engine works against the other. The two
// implementations are separate: TENT's metrics are labeled per transport and
// carry their own config surface, while this exporter deals in unlabeled
// series configured from the environment.
//
// Built only when WITH_METRICS is on: CMake drops this TU otherwise, and every
// include of transfer_engine_metrics.h is guarded to match.

#include <csignal>  // Required before ylt headers: coro_io.hpp uses SIGPIPE
#include <ylt/metric/counter.hpp>
#include <ylt/metric/gauge.hpp>
#include <ylt/metric/histogram.hpp>

// Forward-declare the server so this header stays cheap to include; the full
// type is only needed in the .cpp. ylt ships the coro_http server in namespace
// cinatra and aliases it as coro_http (see ylt/coro_http/coro_http_server.hpp).
namespace cinatra {
class coro_http_server;
}  // namespace cinatra

namespace mooncake {
namespace metrics {

/**
 * @brief A ylt histogram plus the two things ylt keeps to itself.
 *
 * histogram_t exposes its buckets but neither its boundaries nor its sum;
 * the sum only ever leaves through serialize(). /metrics/json needs both
 * (TENT's JSON carries count, sum and buckets), so record the sum alongside
 * and keep the boundaries the histogram was built with.
 */
struct Histogram {
    Histogram(std::string name, std::string help, std::vector<double> buckets)
        : boundaries(buckets),
          histogram(std::move(name), std::move(help), std::move(buckets)) {}

    void observe(int64_t value) {
        histogram.observe(value);
        sum.fetch_add(value, std::memory_order_relaxed);
    }

    // Number of samples, i.e. the sum of the bucket counts.
    int64_t count();

    const std::vector<double> boundaries;
    ylt::metric::histogram_t histogram;
    std::atomic<int64_t> sum{0};
};

/**
 * @brief Configuration for a MetricsExporter instance.
 *
 * Defaults are deliberately conservative: no HTTP server and no periodic log
 * line unless the owner asks for them.
 */
struct ExporterConfig {
    std::string http_host = "0.0.0.0";
    // 0 means "do not start the HTTP server"; metrics are still collected and
    // can be read in-process.
    uint16_t http_port = 0;
    uint16_t http_server_threads = 1;
    // 0 means "do not run the periodic summary log line".
    uint32_t report_interval_seconds = 0;

    /**
     * @brief Read a config from environment variables under `prefix`.
     *
     * For prefix "MC_TE_METRIC" the recognized variables are
     * MC_TE_METRIC_HTTP_HOST, MC_TE_METRIC_HTTP_PORT,
     * MC_TE_METRIC_HTTP_THREADS and MC_TE_METRIC_REPORT_INTERVAL_SECONDS.
     * Unset or malformed values keep the default and log a warning.
     */
    static ExporterConfig fromEnv(const std::string& prefix);
};

/**
 * @brief Registry of metrics plus the HTTP endpoints that expose them.
 *
 * Usage:
 *   exporter_.addCounter(&bytes_total_);
 *   exporter_.addHistogram(&latency_us_);
 *   exporter_.start(ExporterConfig::fromEnv("MC_TE_METRIC"));
 *
 * Registration must happen before start(); the registry is not locked, because
 * it is written once during a component's initialization and only read
 * afterwards (serialization walks it from HTTP handler threads).
 *
 * Thread safety: start()/stop() are idempotent and safe to call concurrently.
 * The record path belongs to the owner, not to this class: ylt counters and
 * histograms are themselves atomic.
 */
class MetricsExporter {
   public:
    // `name` labels log lines, e.g. "Transfer Engine".
    explicit MetricsExporter(std::string name);
    ~MetricsExporter();

    MetricsExporter(const MetricsExporter&) = delete;
    MetricsExporter& operator=(const MetricsExporter&) = delete;

    // Register metrics for serialization. Pointers must outlive the exporter,
    // which is the case when both are members of the same owner.
    void addCounter(ylt::metric::counter_t* counter);
    void addGauge(ylt::metric::gauge_t* gauge);
    // Histogram carries its own bucket boundaries (ylt does not expose them,
    // and the JSON endpoint needs them for bucket labels).
    void addHistogram(Histogram* histogram);

    // Body of the /metrics/summary endpoint and of the periodic log line.
    void setSummaryProvider(std::function<std::string()> provider);

    // Start collecting and, if config.http_port != 0, serve the endpoints.
    // Returns true when the requested configuration is in effect: either no
    // HTTP server was asked for, or it is listening on config.http_port. A
    // failure to bind is logged and returns false, but does not stop
    // collection: the periodic log line and the in-process getters still work,
    // so a port clash never costs you the metrics themselves. A repeat call
    // restarts nothing that is running and reports whether the running
    // exporter satisfies `config`; if it asks for a server and none is
    // listening (an earlier bind failed), the bind is retried with its HTTP
    // settings.
    bool start(const ExporterConfig& config);

    // Stop the HTTP server and the report thread, and stop collecting.
    void stop();

    // Owners gate their record path on this, so it is a single atomic load.
    bool isCollecting() const {
        return collecting_.load(std::memory_order_acquire);
    }

    // Port the HTTP server actually bound to, 0 when no server is running -
    // either because config.http_port was 0 or because the bind failed. Safe
    // to read from other threads while start() is still binding, as TENT's
    // TentMetrics::httpPort() is.
    uint16_t httpPort() const {
        return bound_http_port_.load(std::memory_order_relaxed);
    }

    // Endpoint bodies, also usable in-process (e.g. from tests) without an
    // HTTP server. summaryText() is empty unless a provider was set.
    std::string prometheusText();
    std::string jsonText();
    std::string summaryText();

   private:
    std::string name_;
    ExporterConfig config_;
    std::atomic<bool> collecting_{false};
    // Serializes start()/stop(); the record path only reads collecting_.
    std::mutex lifecycle_mutex_;
    // Kept separate from config_.http_port, which is what was asked for rather
    // than what was bound, and reset on stop() so a later failed bind cannot
    // report a stale port.
    std::atomic<uint16_t> bound_http_port_{0};

    void startHttpServer();
    void startReportThread();

    std::vector<ylt::metric::counter_t*> counters_;
    std::vector<ylt::metric::gauge_t*> gauges_;
    std::vector<Histogram*> histograms_;
    std::function<std::string()> summary_provider_;

    std::unique_ptr<cinatra::coro_http_server> http_server_;

    std::thread report_thread_;
    std::atomic<bool> report_running_{false};
    std::mutex report_mutex_;
    std::condition_variable report_cv_;
};

}  // namespace metrics
}  // namespace mooncake

#endif  // MOONCAKE_METRICS_EXPORTER_H
