//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_STREAMING_BENCHMARK_H
#define ENJIMA_BENCHMARKS_STREAMING_BENCHMARK_H

#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/scheduling/SchedulingTypes.h"
#include "enjima/runtime/CancellationException.h"

#include <cstdint>
#include <string>

namespace enjima::benchmarks::workload {
    using TimestampT = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>;

    class StreamingBenchmark {
    public:
        explicit StreamingBenchmark(std::string benchmarkName);
        virtual ~StreamingBenchmark();
        void Initialize(unsigned int iteration, size_t maxMemory, int32_t numEventsPerBlock, size_t numBlocksPerChunk,
                int32_t backPressureThreshold, const std::string& schedulingModeStr, std::string systemIDString,
                uint64_t schedulingPeriodMs = 0, uint32_t numWorkers = 8,
                const std::string& processingModeStr = "BlockBasedSingle", int numQueries = 1);
        void StopBenchmark();
        virtual void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) = 0;
        virtual void RunBenchmark(uint64_t durationInSec) = 0;
        [[nodiscard]] const std::string& GetBenchmarkName() const;

        void SetPreemptMode(const std::string& preemptModeStr);
        void SetPriorityType(const std::string& priorityTypeStr);
        void SetMaxIdleThresholdMs(uint64_t maxIdleThresholdMs);
        void SetSourceReservoirCapacity(uint64_t srcReservoirCapacity);

    protected:
        static runtime::StreamingTask::ProcessingMode GetProcessingMode(const std::string& processingModeStr);
        static runtime::SchedulingMode GetSchedulingMode(const std::string& schedulingModeStr);
        static runtime::PriorityType GetPriorityType(const std::string& priorityTypeStr);
        static runtime::PreemptMode GetPreemptMode(const std::string& preemptModeStr);
        static std::string GetSuffixedOperatorName(const std::string& opName, const std::string& jobIdSuffix);

        const std::string srcOpName_;
        const std::string sinkOpName_;

        enjima::runtime::ExecutionEngine* executionEngine_{nullptr};
        enjima::memory::MemoryManager* pMemoryManager_{nullptr};
        enjima::metrics::Profiler* profilerPtr_{nullptr};
        std::vector<enjima::runtime::StreamingJob*> benchmarkStreamingJobs_;
        std::vector<enjima::core::JobID> queryIds_;
        enjima::runtime::StreamingTask::ProcessingMode processingMode_{
                runtime::StreamingTask::ProcessingMode::kUnassigned};
        enjima::runtime::SchedulingMode schedulingMode_{runtime::SchedulingMode::kThreadBased};
        enjima::runtime::PriorityType priorityType_{runtime::PriorityType::kInputQueueSize};
        enjima::runtime::PreemptMode preemptMode_{runtime::PreemptMode::kNonPreemptive};
        uint64_t maxIdleThresholdMs_{0};
        uint64_t srcReservoirCapacity_{1'000'000};
        TimestampT startTime_;
        TimestampT endTime_;
        unsigned int iter_{0};

    private:
        static std::vector<std::string> GetTargetValuesAsTokenizedVectorFromFile(const std::string& filename,
                enjima::metrics::Profiler* pProf, const std::string& targetVal, size_t targetIdx, size_t metricNameIdx);
        void PrintThroughputResults();
        void PrintLatencyResults();
        void writeResultToFile(uint64_t srcOutCount, const std::chrono::seconds& timeDurationSec,
                double throughput) const;
        void PrintAndWriteSummaryResults();

        const std::string benchmarkName_;
    };
}// namespace enjima::benchmarks::workload

#endif//ENJIMA_BENCHMARKS_STREAMING_BENCHMARK_H
