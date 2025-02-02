//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_LINEAR_ROAD_BENCHMARK_H
#define ENJIMA_BENCHMARKS_LINEAR_ROAD_BENCHMARK_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/LRBFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedLRBSourceOperator.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/FixedEventTimeWindowCoGroupOperator.h"
#include "enjima/operators/FixedEventTimeWindowJoinOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/SlidingEventTimeWindowOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingLRBSrcOpT = enjima::operators::LatencyTrackingSourceOperator<LinearRoadT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class LinearRoadBenchmark : public StreamingBenchmark {
    public:
        LinearRoadBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(const std::string& path);

    private:
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingLRBSrcOpT<Duration>, T>
        void SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        const std::string filterOpName_ = "typeFilter";
        const std::string projOpName_ = "project";
        const std::string multiMapOpName_ = "splitMap";
        const std::string acdFilterOpName_ = "travelFilter";
        const std::string acdWindowOpName_ = "accidentWindow";
        const std::string spdWindowOpName_ = "speedWindow";
        const std::string cntWindowOpName_ = "countWindow";
        const std::string cntSpdWindowJoinOpName_ = "countAndSpeedJoin";
        const std::string acdTollCoGroupOpName_ = "tollAndAccidentCoGroup";
        std::string lrbDataPath_;
        std::vector<LinearRoadT> eventCache_;
    };
}// namespace enjima::benchmarks::workload

#include "LinearRoadBenchmark.tpp"

#endif//ENJIMA_BENCHMARKS_LINEAR_ROAD_BENCHMARK_H
