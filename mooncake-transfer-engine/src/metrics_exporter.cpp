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

#include "metrics_exporter.h"

#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <exception>
#include <limits>
#include <sstream>
#include <utility>

#include <ylt/coro_http/coro_http_server.hpp>

namespace mooncake {
namespace metrics {

namespace {

// Pre-allocation hint for Prometheus serialization, as in TENT.
constexpr size_t kPrometheusBufferSize = 4096;

const char* getEnvValue(const std::string& prefix, const char* suffix) {
    std::string name = prefix + suffix;
    const char* value = std::getenv(name.c_str());
    if (!value || *value == '\0') return nullptr;
    return value;
}

// Parse an unsigned env var into T, keeping `fallback` when the variable is
// unset, malformed or out of T's range.
template <typename T>
T getEnvUnsigned(const std::string& prefix, const char* suffix, T fallback) {
    const char* value = getEnvValue(prefix, suffix);
    if (!value) return fallback;
    try {
        size_t consumed = 0;
        long long parsed = std::stoll(value, &consumed);
        // Reject trailing garbage ("9100abc", "1e3"), not just non-numbers.
        if (value[consumed] != '\0' || parsed < 0 ||
            static_cast<unsigned long long>(parsed) >
                std::numeric_limits<T>::max()) {
            LOG(WARNING) << "Ignoring invalid " << prefix << suffix << "="
                         << value << ", using " << fallback;
            return fallback;
        }
        return static_cast<T>(parsed);
    } catch (const std::exception&) {
        LOG(WARNING) << "Failed to parse " << prefix << suffix << "=" << value
                     << ", using " << fallback;
        return fallback;
    }
}

}  // namespace

ExporterConfig ExporterConfig::fromEnv(const std::string& prefix) {
    ExporterConfig config;
    if (const char* host = getEnvValue(prefix, "_HTTP_HOST")) {
        config.http_host = host;
    }
    config.http_port = getEnvUnsigned(prefix, "_HTTP_PORT", config.http_port);
    config.http_server_threads =
        getEnvUnsigned(prefix, "_HTTP_THREADS", config.http_server_threads);
    if (config.http_server_threads == 0) {
        LOG(WARNING) << "Ignoring " << prefix
                     << "_HTTP_THREADS=0, using 1 thread";
        config.http_server_threads = 1;
    }
    config.report_interval_seconds = getEnvUnsigned(
        prefix, "_REPORT_INTERVAL_SECONDS", config.report_interval_seconds);
    return config;
}

MetricsExporter::MetricsExporter(std::string name) : name_(std::move(name)) {}

MetricsExporter::~MetricsExporter() { stop(); }

void MetricsExporter::addCounter(ylt::metric::counter_t* counter) {
    if (counter) counters_.push_back(counter);
}

void MetricsExporter::addGauge(ylt::metric::gauge_t* gauge) {
    if (gauge) gauges_.push_back(gauge);
}

void MetricsExporter::addHistogram(Histogram* histogram) {
    if (histogram) histograms_.push_back(histogram);
}

int64_t Histogram::count() {
    int64_t total = 0;
    for (auto& bucket : histogram.get_bucket_counts()) {
        total += bucket->value();
    }
    return total;
}

void MetricsExporter::setSummaryProvider(
    std::function<std::string()> provider) {
    summary_provider_ = std::move(provider);
}

bool MetricsExporter::start(const ExporterConfig& config) {
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    bool expected = false;
    if (!collecting_.compare_exchange_strong(expected, true)) {
        // Already started. Nothing that is running is restarted, but a server
        // that is not running (an earlier bind failed, typically a port clash
        // at startup, or the earlier caller wanted none) is tried now with
        // this caller's HTTP settings: nothing is listening, so there is
        // nothing to conflict with, and returning early would leave the
        // process without a scrape endpoint for good.
        if (config.http_port != 0 && !http_server_) {
            config_.http_host = config.http_host;
            config_.http_port = config.http_port;
            config_.http_server_threads = config.http_server_threads;
            startHttpServer();
        }
        // Report whether what is running matches what this caller asked for.
        return config.http_port == 0 || httpPort() == config.http_port;
    }

    config_ = config;

    if (config_.http_port != 0) {
        startHttpServer();
    }
    if (config_.report_interval_seconds > 0) {
        startReportThread();
    }

    if (httpPort() != 0) {
        LOG(INFO) << name_ << " metrics started (http=" << config_.http_host
                  << ":" << httpPort()
                  << ", report_interval=" << config_.report_interval_seconds
                  << "s)";
    } else {
        LOG(INFO) << name_ << " metrics started (in-process only"
                  << ", report_interval=" << config_.report_interval_seconds
                  << "s)";
    }
    return config_.http_port == 0 || httpPort() != 0;
}

void MetricsExporter::startHttpServer() {
    using namespace cinatra;
    try {
        http_server_ = std::make_unique<coro_http_server>(
            config_.http_server_threads, config_.http_port, config_.http_host);

        http_server_->set_http_handler<GET>(
            "/metrics", [this](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok, prometheusText());
            });
        http_server_->set_http_handler<GET>(
            "/metrics/summary",
            [this](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain");
                resp.set_status_and_content(status_type::ok, summaryText());
            });
        http_server_->set_http_handler<GET>(
            "/metrics/json",
            [this](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "application/json");
                resp.set_status_and_content(status_type::ok, jsonText());
            });
        http_server_->set_http_handler<GET>(
            "/health", [](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain");
                resp.set_status_and_content(status_type::ok, "OK");
            });

        // async_start() binds synchronously and hands back a future that is
        // already resolved (hasResult()) only when the bind failed; on success
        // it stays pending while the server runs. It does not throw, so
        // ignoring this would report a listening endpoint that is not there.
        // Same idiom as tent_metrics.cpp and mooncake-store's
        // http_metadata_server.cpp.
        auto ec = http_server_->async_start();
        if (ec.hasResult()) {
            // Keep collecting: a port clash must not cost the metrics.
            LOG(ERROR) << "Failed to bind " << name_
                       << " metrics HTTP server to " << config_.http_host << ":"
                       << config_.http_port
                       << ". Metrics are still collected in-process.";
            http_server_.reset();
            return;
        }

        bound_http_port_.store(config_.http_port, std::memory_order_relaxed);
        LOG(INFO) << name_ << " metrics HTTP server listening on "
                  << config_.http_host << ":" << config_.http_port;
    } catch (const std::exception& e) {
        // The server constructor can still throw, e.g. on a bad host string.
        LOG(ERROR) << "Failed to start " << name_ << " metrics HTTP server on "
                   << config_.http_host << ":" << config_.http_port << ": "
                   << e.what() << ". Metrics are still collected in-process.";
        http_server_.reset();
    }
}

void MetricsExporter::startReportThread() {
    report_running_.store(true, std::memory_order_relaxed);
    report_thread_ = std::thread([this]() {
        while (report_running_.load(std::memory_order_relaxed)) {
            LOG(INFO) << name_ << " metrics: " << summaryText();
            std::unique_lock<std::mutex> lock(report_mutex_);
            report_cv_.wait_for(
                lock, std::chrono::seconds(config_.report_interval_seconds),
                [this]() {
                    return !report_running_.load(std::memory_order_relaxed);
                });
        }
    });
}

void MetricsExporter::stop() {
    // Under the lock too: without it a start() racing this stop() could see
    // collecting_ already false and assign a new thread over the still
    // joinable report_thread_, which is std::terminate.
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    if (!collecting_.exchange(false)) return;

    {
        // Under the lock, or the store can slip between the report thread's
        // predicate check and its wait and the wakeup is lost for one interval.
        std::lock_guard<std::mutex> lock(report_mutex_);
        report_running_.store(false, std::memory_order_relaxed);
    }
    report_cv_.notify_all();
    if (report_thread_.joinable()) {
        report_thread_.join();
    }

    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }
    bound_http_port_.store(0, std::memory_order_relaxed);
}

std::string MetricsExporter::prometheusText() {
    if (!isCollecting()) return "";
    try {
        std::string result;
        result.reserve(kPrometheusBufferSize);
        for (auto* counter : counters_) {
            counter->serialize(result);
        }
        for (auto* gauge : gauges_) {
            gauge->serialize(result);
        }
        for (auto* entry : histograms_) {
            // NOTE: ylt omits a histogram whose accumulated sum is 0, header
            // included, so a metric stays absent from /metrics until its
            // first nonzero sample (histogram.hpp: `if (val == 0) return;`).
            // /metrics/json below reports the sample count regardless.
            entry->histogram.serialize(result);
        }
        return result;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to serialize " << name_
                   << " Prometheus metrics: " << e.what();
        return "";
    }
}

std::string MetricsExporter::jsonText() {
    if (!isCollecting()) return "{}";
    std::ostringstream oss;
    oss << "{";
    bool first = true;
    auto separator = [&]() {
        if (!first) oss << ",";
        first = false;
    };

    for (auto* counter : counters_) {
        separator();
        oss << "\"" << counter->str_name() << "\":" << counter->value();
    }
    for (auto* gauge : gauges_) {
        separator();
        oss << "\"" << gauge->str_name() << "\":" << gauge->value();
    }
    for (auto* entry : histograms_) {
        auto bucket_counts = entry->histogram.get_bucket_counts();
        separator();
        oss << "\"" << entry->histogram.str_name()
            << "\":{\"count\":" << entry->count()
            << ",\"sum\":" << entry->sum.load(std::memory_order_relaxed)
            << ",\"buckets\":{";
        for (size_t i = 0;
             i < entry->boundaries.size() && i < bucket_counts.size(); ++i) {
            if (i > 0) oss << ",";
            oss << "\"" << static_cast<int64_t>(entry->boundaries[i])
                << "\":" << bucket_counts[i]->value();
        }
        oss << "}}";
    }
    oss << "}";
    return oss.str();
}

std::string MetricsExporter::summaryText() {
    if (!isCollecting()) return "Metrics not started";
    return summary_provider_ ? summary_provider_() : std::string();
}

}  // namespace metrics
}  // namespace mooncake
