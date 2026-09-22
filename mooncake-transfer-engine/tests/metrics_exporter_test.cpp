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

// The HTTP half of the metrics stack: config parsing, binding, the four
// endpoints, and what happens when the port cannot be had.
// transfer_engine_metrics_test.cpp covers the accounting that feeds it.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <csignal>  // required before ylt headers (coro_io uses std::signal)
#include <ylt/coro_http/coro_http_client.hpp>

#include "metrics_exporter.h"
#include "transfer_engine_metrics.h"

namespace mooncake {
namespace {

// Bind a loopback socket on port 0, read the assigned port, close, return it.
// The TOCTOU race is inherent; callers re-check via httpPort().
// Adapted from mooncake-store/src/utils.cpp getFreeTcpPort().
uint16_t getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return 0;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(sock);
        return 0;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return static_cast<uint16_t>(port);
}

// RAII helper that holds a listening socket, keeping its port busy for the
// duration of a test. Adapted from tent/tests/metrics_http_server_test.cpp.
class PortOccupier {
   public:
    PortOccupier() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(fd_, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // let the kernel pick a free port
        EXPECT_EQ(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
                  0);
        EXPECT_EQ(::listen(fd_, 1), 0);

        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        EXPECT_EQ(::getsockname(fd_, reinterpret_cast<sockaddr*>(&bound), &len),
                  0);
        port_ = ntohs(bound.sin_port);
    }

    ~PortOccupier() { release(); }

    // Give the port back early.
    void release() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

    uint16_t port() const { return port_; }

   private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

struct HttpResponse {
    int http_status;
    std::string body;
};

// Adapted from mooncake-store/tests/master_metrics_test.cpp FetchUrl().
HttpResponse fetchUrl(uint16_t port, const std::string& path) {
    coro_http::coro_http_client client;
    auto result = client.get("http://127.0.0.1:" + std::to_string(port) + path);
    return {result.status, std::string(result.resp_body)};
}

// Retry a GET to absorb the window between async_start() returning and the
// server accepting connections.
HttpResponse retryGet(uint16_t port, const std::string& path,
                      int attempts = 20) {
    HttpResponse resp{0, ""};
    for (int i = 0; i < attempts; ++i) {
        resp = fetchUrl(port, path);
        if (resp.http_status == 200) return resp;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return resp;
}

// Stands in for a component's own metric set, so the exporter is exercised
// without TransferEngineMetrics' singleton state.
struct OwnerMetrics {
    ylt::metric::counter_t requests_total{"exporter_test_requests_total",
                                          "Requests seen by the test owner"};
    ylt::metric::gauge_t inflight{"exporter_test_inflight",
                                  "Requests not yet terminal"};
    metrics::Histogram latency_us{"exporter_test_latency_us",
                                  "Latency in microseconds",
                                  std::vector<double>{100, 1000, 10000}};
};

class MetricsExporterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        exporter_.addCounter(&owner_.requests_total);
        exporter_.addGauge(&owner_.inflight);
        exporter_.addHistogram(&owner_.latency_us);
        exporter_.setSummaryProvider(
            []() { return std::string("owner summary line"); });
    }

    void TearDown() override { exporter_.stop(); }

    // A config that binds `port` on loopback and runs no report thread.
    static metrics::ExporterConfig localConfig(uint16_t port) {
        metrics::ExporterConfig config;
        config.http_host = "127.0.0.1";
        config.http_port = port;
        config.report_interval_seconds = 0;
        return config;
    }

    void recordSomething() {
        owner_.requests_total.inc();
        owner_.inflight.inc();
        owner_.latency_us.observe(250);
    }

    metrics::MetricsExporter exporter_{"Exporter Test"};
    OwnerMetrics owner_;
};

// --- Config ----------------------------------------------------------------

TEST_F(MetricsExporterTest, FromEnvReadsHttpVariables) {
    setenv("MC_TE_METRIC_TEST_HTTP_HOST", "127.0.0.1", 1);
    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "19321", 1);
    setenv("MC_TE_METRIC_TEST_HTTP_THREADS", "3", 1);
    setenv("MC_TE_METRIC_TEST_REPORT_INTERVAL_SECONDS", "7", 1);

    auto config = metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST");
    EXPECT_EQ(config.http_host, "127.0.0.1");
    EXPECT_EQ(config.http_port, 19321);
    EXPECT_EQ(config.http_server_threads, 3);
    EXPECT_EQ(config.report_interval_seconds, 7u);

    unsetenv("MC_TE_METRIC_TEST_HTTP_HOST");
    unsetenv("MC_TE_METRIC_TEST_HTTP_PORT");
    unsetenv("MC_TE_METRIC_TEST_HTTP_THREADS");
    unsetenv("MC_TE_METRIC_TEST_REPORT_INTERVAL_SECONDS");
}

// A malformed or out-of-range value must not silently become a port the owner
// did not ask for; the default (no server) stands.
TEST_F(MetricsExporterTest, FromEnvKeepsDefaultsOnBadValues) {
    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "not-a-port", 1);
    setenv("MC_TE_METRIC_TEST_HTTP_THREADS", "0", 1);

    auto config = metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST");
    EXPECT_EQ(config.http_port, 0);
    EXPECT_EQ(config.http_server_threads, 1);  // 0 threads is coerced to 1

    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "70000", 1);  // > uint16 max
    EXPECT_EQ(metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST").http_port,
              0);

    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "9100abc", 1);  // trailing garbage
    EXPECT_EQ(metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST").http_port,
              0);

    unsetenv("MC_TE_METRIC_TEST_HTTP_PORT");
    unsetenv("MC_TE_METRIC_TEST_HTTP_THREADS");
}

TEST_F(MetricsExporterTest, PortZeroCollectsWithoutAServer) {
    ASSERT_TRUE(exporter_.start(localConfig(0)));

    EXPECT_TRUE(exporter_.isCollecting());
    EXPECT_EQ(exporter_.httpPort(), 0);

    recordSomething();
    EXPECT_NE(exporter_.prometheusText().find("exporter_test_requests_total"),
              std::string::npos);
}

// --- Endpoints -------------------------------------------------------------

TEST_F(MetricsExporterTest, ServesAllFourEndpoints) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    if (!exporter_.start(localConfig(port))) {
        GTEST_SKIP() << "metrics HTTP server did not bind port " << port;
    }
    ASSERT_EQ(exporter_.httpPort(), port);

    recordSomething();

    auto health = retryGet(port, "/health");
    EXPECT_EQ(health.http_status, 200);
    EXPECT_EQ(health.body, "OK");

    auto prom = retryGet(port, "/metrics");
    EXPECT_EQ(prom.http_status, 200);
    EXPECT_NE(prom.body.find("exporter_test_requests_total"),
              std::string::npos);
    EXPECT_NE(prom.body.find("exporter_test_inflight"), std::string::npos);
    // The histogram has a nonzero sum by now, so ylt emits it.
    EXPECT_NE(prom.body.find("exporter_test_latency_us_sum"),
              std::string::npos);
    EXPECT_NE(prom.body.find("exporter_test_latency_us_count"),
              std::string::npos);

    auto json = retryGet(port, "/metrics/json");
    EXPECT_EQ(json.http_status, 200);
    EXPECT_NE(json.body.find("exporter_test_requests_total"),
              std::string::npos);
    // Histograms carry count, sum and buckets, as in TENT's /metrics/json.
    EXPECT_NE(json.body.find("\"exporter_test_latency_us\":{\"count\":1,"
                             "\"sum\":250,\"buckets\":{"),
              std::string::npos)
        << json.body;

    auto summary = retryGet(port, "/metrics/summary");
    EXPECT_EQ(summary.http_status, 200);
    EXPECT_EQ(summary.body, "owner summary line");
}

// The scrape must reflect the current values, not a snapshot taken at start().
TEST_F(MetricsExporterTest, ScrapeReflectsLaterRecording) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    if (!exporter_.start(localConfig(port))) {
        GTEST_SKIP() << "metrics HTTP server did not bind port " << port;
    }

    recordSomething();
    auto first = retryGet(port, "/metrics");
    ASSERT_EQ(first.http_status, 200);
    EXPECT_NE(first.body.find("exporter_test_requests_total 1"),
              std::string::npos);

    recordSomething();
    auto second = retryGet(port, "/metrics");
    ASSERT_EQ(second.http_status, 200);
    EXPECT_NE(second.body.find("exporter_test_requests_total 2"),
              std::string::npos);
}

// --- Degradation -----------------------------------------------------------

// A busy port must not be reported as a listening endpoint. cinatra's
// async_start() signals a failed bind through its future rather than by
// throwing, so this is the regression test for treating that future as
// success: start() returns false, httpPort() stays 0 and collection carries
// on in-process.
TEST_F(MetricsExporterTest, BusyPortDegradesToInProcessCollection) {
    PortOccupier occupier;
    ASSERT_GT(occupier.port(), 0);

    EXPECT_FALSE(exporter_.start(localConfig(occupier.port())));
    EXPECT_TRUE(exporter_.isCollecting());
    EXPECT_EQ(exporter_.httpPort(), 0);

    recordSomething();
    EXPECT_NE(exporter_.prometheusText().find("exporter_test_requests_total"),
              std::string::npos);
}

// Two ranks racing for one MC_TE_METRIC_HTTP_PORT: the loser's bind fails at
// startup. A later start() in that process (another engine, or the same env
// once the clash clears) must try the bind again rather than treat the
// request as already served, or the process never gets a scrape endpoint.
TEST_F(MetricsExporterTest, RepeatStartRetriesAFailedBind) {
    PortOccupier occupier;
    ASSERT_GT(occupier.port(), 0);
    const uint16_t port = occupier.port();

    EXPECT_FALSE(exporter_.start(localConfig(port)));
    EXPECT_FALSE(exporter_.start(localConfig(port)));  // still busy
    EXPECT_EQ(exporter_.httpPort(), 0);

    occupier.release();
    if (!exporter_.start(localConfig(port))) {
        GTEST_SKIP() << "metrics HTTP server did not bind port " << port;
    }
    EXPECT_EQ(exporter_.httpPort(), port);
    recordSomething();
    auto response = retryGet(port, "/metrics");
    EXPECT_EQ(response.http_status, 200);
    EXPECT_NE(response.body.find("exporter_test_requests_total"),
              std::string::npos);
}

// start() restarts nothing that is running. A repeat call's return value
// tells the second caller whether the running exporter already serves what
// it asked for.
TEST_F(MetricsExporterTest, RepeatStartReportsWhetherRequestIsSatisfied) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    if (!exporter_.start(localConfig(port))) {
        GTEST_SKIP() << "metrics HTTP server did not bind port " << port;
    }

    EXPECT_TRUE(exporter_.start(localConfig(port)));  // same request
    EXPECT_TRUE(exporter_.start(localConfig(0)));     // asks for nothing more
    EXPECT_FALSE(exporter_.start(localConfig(port + 1)));  // not what runs
    EXPECT_EQ(exporter_.httpPort(), port);
}

TEST_F(MetricsExporterTest, StopClearsBoundPortAndReleasesIt) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    if (!exporter_.start(localConfig(port))) {
        GTEST_SKIP() << "metrics HTTP server did not bind port " << port;
    }

    exporter_.stop();
    EXPECT_EQ(exporter_.httpPort(), 0);
    EXPECT_FALSE(exporter_.isCollecting());

    // The socket is gone, so a second exporter can take the same port. This is
    // what makes httpPort() trustworthy after a restart.
    metrics::MetricsExporter restarted("Exporter Test Restarted");
    restarted.addCounter(&owner_.requests_total);
    EXPECT_TRUE(restarted.start(localConfig(port)));
    EXPECT_EQ(restarted.httpPort(), port);
    restarted.stop();
}

// --- Engine wiring ---------------------------------------------------------

using Direction = TransferEngineMetrics::Direction;

// End to end through the owner the exporter was written for: the environment
// variables an operator sets must produce a scrapable mooncake_te_ endpoint.
class TransferEngineMetricsHttpTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto& metrics = TransferEngineMetrics::instance();
        metrics.shutdown();  // start() is first-one-wins; drop any prior server
        metrics.resetForTesting();

        port_ = getFreeTcpPort();
        ASSERT_GT(port_, 0);
        setenv("MC_TE_METRIC_HTTP_HOST", "127.0.0.1", 1);
        setenv("MC_TE_METRIC_HTTP_PORT", std::to_string(port_).c_str(), 1);
        unsetenv("MC_TE_METRIC_REPORT_INTERVAL_SECONDS");
        metrics.initializeFromEnv();

        if (metrics.httpPort() == 0) {
            GTEST_SKIP() << "metrics HTTP server did not bind port " << port_;
        }
    }

    void TearDown() override {
        TransferEngineMetrics::instance().shutdown();
        unsetenv("MC_TE_METRIC_HTTP_HOST");
        unsetenv("MC_TE_METRIC_HTTP_PORT");
    }

    uint16_t port_ = 0;
};

TEST_F(TransferEngineMetricsHttpTest, RecordedTransfersAreScrapable) {
    auto& metrics = TransferEngineMetrics::instance();
    metrics.recordCompleted(Direction::Read, 65536, 1500);
    metrics.recordFailed(Direction::Write);

    auto resp = retryGet(port_, "/metrics");
    ASSERT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("mooncake_te_read_bytes_total 65536"),
              std::string::npos);
    EXPECT_NE(resp.body.find("mooncake_te_write_failures_total 1"),
              std::string::npos);
    EXPECT_NE(resp.body.find("mooncake_te_read_latency_us_sum"),
              std::string::npos);
    EXPECT_NE(resp.body.find("mooncake_te_read_size_bytes_count"),
              std::string::npos);

    auto health = retryGet(port_, "/health");
    EXPECT_EQ(health.http_status, 200);
    EXPECT_EQ(health.body, "OK");
}

}  // namespace
}  // namespace mooncake
