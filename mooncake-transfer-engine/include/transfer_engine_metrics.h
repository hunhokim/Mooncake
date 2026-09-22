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

#ifndef TRANSFER_ENGINE_METRICS_H_
#define TRANSFER_ENGINE_METRICS_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

#include "metrics_exporter.h"

namespace mooncake {

/**
 * @brief Prometheus metrics for the Classic Transfer Engine.
 *
 * Process-global rather than a member of TransferEngineImpl, because the
 * exporter binds a single port and serves a single /metrics: several engines
 * in one process (paired initiator/target tests, one engine per GPU rank, the
 * throwaway engine behind TransferEnginePy::getLocalTopology) report into one
 * scrape target. This mirrors TENT's TentMetrics. The cost is that the series
 * are not broken down per engine; adding a `segment` label later is confined
 * to the record calls in MultiTransport.
 *
 * All the scaffolding (configuration, the /metrics, /metrics/summary,
 * /metrics/json and /health endpoints, serialization and the collecting switch)
 * comes from mooncake::metrics::MetricsExporter. This class only declares the
 * metric set and the record entry points.
 *
 * The metric set follows TENT's read/write split, with TENT's bucket
 * boundaries, under a mooncake_te_ prefix:
 *
 *   mooncake_te_{read,write}_bytes_total       counter
 *   mooncake_te_{read,write}_requests_total    counter (completed + failed)
 *   mooncake_te_{read,write}_failures_total    counter
 *   mooncake_te_{read,write}_latency_us        histogram (microseconds)
 *   mooncake_te_{read,write}_size_bytes        histogram (bytes)
 *
 * Contract: the metrics are observation-based. A task is recorded once, at
 * the first status query (per-task or batch) that observes it in a terminal
 * state; see MultiTransport::recordTaskTerminal(). A task nobody polls to
 * completion is not recorded at all, so there is no in-flight gauge and no
 * identity between submissions and recorded outcomes. Within what is
 * observed, requests == latency_us_count + failures holds by construction.
 */
class TransferEngineMetrics {
   public:
    // Direction of a transfer, mapped from TransferRequest::OpCode by the
    // caller so this header does not have to include transport.h.
    enum class Direction { Read, Write };

    static TransferEngineMetrics& instance();

    /**
     * @brief Start collecting, reading the HTTP configuration from the
     *        MC_TE_METRIC_* environment variables.
     *
     * Idempotent: only the first call takes effect, so several Transfer Engine
     * instances in one process share a single exporter and a single port. A
     * later engine whose configuration differs from the first one's is warned
     * about rather than silently ignored. Callers gate this on MC_TE_METRIC,
     * the existing Classic TE metrics switch.
     */
    void initializeFromEnv();

    // Stops the exporter and re-arms initializeFromEnv(), so that a later
    // initializeFromEnv() starts a fresh server rather than being treated as a
    // second engine joining the first one's. Tests rely on this to rebind a
    // new port per case.
    void shutdown() {
        std::lock_guard<std::mutex> guard(init_mutex_);
        exporter_.stop();
        initialized_ = false;
        initial_config_ = metrics::ExporterConfig{};
    }

    // True once initializeFromEnv() has run, i.e. when the record* calls do
    // work. Every recorder checks this itself; callers need it only to skip
    // the work of preparing a record.
    bool isRecording() const { return exporter_.isCollecting(); }

    // A task was observed COMPLETED. `bytes` is the number of bytes the
    // transport reports as transferred; `latency_us` is measured from
    // submission to this observation.
    void recordCompleted(Direction direction, size_t bytes,
                         uint64_t latency_us);

    // A task was observed FAILED / CANCELED / TIMEOUT.
    void recordFailed(Direction direction);

    // Port the metrics HTTP server bound to, 0 when it is not serving (no
    // MC_TE_METRIC_HTTP_PORT, or the bind failed and metrics stayed
    // in-process).
    uint16_t httpPort() const { return exporter_.httpPort(); }

    // Endpoint bodies, also reachable in-process without an HTTP server.
    std::string prometheusText() { return exporter_.prometheusText(); }
    std::string jsonText() { return exporter_.jsonText(); }
    std::string summaryText() { return exporter_.summaryText(); }

    // The read or the write half of the metric set. Both halves carry the same
    // series, named after their direction.
    struct DirectionMetrics {
        explicit DirectionMetrics(std::string direction);
        void resetForTesting();

        const std::string direction;
        ylt::metric::counter_t bytes_total;
        ylt::metric::counter_t requests_total;
        ylt::metric::counter_t failures_total;
        metrics::Histogram latency_us;
        metrics::Histogram size_bytes;
    };

    // Direct access to the series, for tests and for the summary line.
    DirectionMetrics& forDirection(Direction direction) {
        return direction == Direction::Read ? read_ : write_;
    }

    // Zero every metric. For test isolation only: the singleton outlives
    // individual Transfer Engine instances.
    void resetForTesting();

   private:
    TransferEngineMetrics();
    ~TransferEngineMetrics() = default;
    TransferEngineMetrics(const TransferEngineMetrics&) = delete;
    TransferEngineMetrics& operator=(const TransferEngineMetrics&) = delete;

    // Guards the one-shot initialization below. Contended at most once per
    // engine construction, so a plain mutex is cheap enough; it also keeps
    // initial_config_ from being read while the first caller writes it.
    std::mutex init_mutex_;
    bool initialized_ = false;
    metrics::ExporterConfig initial_config_;

    std::string buildSummary();

    DirectionMetrics read_{"read"};
    DirectionMetrics write_{"write"};

    // Declared last so that it is destroyed first: ~MetricsExporter() joins the
    // report thread and stops the HTTP server, both of which read the metrics
    // above. With the exporter declared earlier those readers would outlive
    // their data, and the singleton is destroyed at process exit with the
    // server still running, since nothing in production calls shutdown().
    // Registration happens in the constructor body, after every member is
    // built, so construction order is unaffected.
    metrics::MetricsExporter exporter_{"Transfer Engine"};
};

}  // namespace mooncake

#endif  // TRANSFER_ENGINE_METRICS_H_
