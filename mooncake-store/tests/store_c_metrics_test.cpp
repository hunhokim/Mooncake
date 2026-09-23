// Copyright 2026 KVCache.AI
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

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>

#include "transfer_engine_metrics.h"

namespace mooncake {
namespace {

TEST(StoreCMetricsTest, SharesMetricsWithLinkedConsumers) {
    auto close_library = [](void* handle) { dlclose(handle); };
    std::unique_ptr<void, decltype(close_library)> library(
        dlopen(MOONCAKE_STORE_C_LIBRARY, RTLD_NOW | RTLD_LOCAL), close_library);
    ASSERT_NE(library, nullptr) << dlerror();
    ASSERT_NE(dlsym(library.get(), "mooncake_store_create"), nullptr)
        << dlerror();

    // Obtain the compiler's symbol spelling instead of hard-coding its C++
    // mangling. Looking it up through the Store handle searches that library's
    // dependencies, not the executable's independently linked metrics library.
    Dl_info symbol{};
    ASSERT_NE(dladdr(reinterpret_cast<void*>(&TransferEngineMetrics::instance),
                     &symbol),
              0);
    ASSERT_NE(symbol.dli_sname, nullptr);
    auto instance =
        reinterpret_cast<decltype(&TransferEngineMetrics::instance)>(
            dlsym(library.get(), symbol.dli_sname));
    ASSERT_NE(instance, nullptr) << dlerror();

    auto& linked_metrics = TransferEngineMetrics::instance();
    auto& store_metrics = instance();
    ASSERT_EQ(&store_metrics, &linked_metrics);
    linked_metrics.resetForTesting();

    // Collection started through the Store dependency must also enable the
    // ordinary consumer, and both must contribute to the same counters.
    setenv("MC_TE_METRIC_HTTP_PORT", "0", 1);
    setenv("MC_TE_METRIC_REPORT_INTERVAL_SECONDS", "0", 1);
    store_metrics.initializeFromEnv();
    EXPECT_TRUE(linked_metrics.isRecording());
    using Direction = TransferEngineMetrics::Direction;
    store_metrics.recordCompleted(Direction::Read, 4096, 100);
    linked_metrics.recordFailed(Direction::Read);
    auto& read = linked_metrics.forDirection(Direction::Read);
    EXPECT_EQ(read.requests_total.value(), 2);
    EXPECT_EQ(read.bytes_total.value(), 4096);
    EXPECT_EQ(read.failures_total.value(), 1);
    EXPECT_EQ(read.latency_us.count(), 1);
    store_metrics.shutdown();
}

}  // namespace
}  // namespace mooncake
