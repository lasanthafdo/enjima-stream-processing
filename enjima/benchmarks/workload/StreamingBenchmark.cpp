//
// Created by m34ferna on 21/02/24.
//

#include "StreamingBenchmark.h"
#include "WorkloadException.h"
#include "enjima/memory/MemoryManager.h"
#include "enjima/metrics/MetricSuffixes.h"
#include "enjima/runtime/StreamingJob.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <utility>

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

namespace enjima::benchmarks::workload {
    static const std::string kPerfResultFilename = "perf_results.csv";
    static const char kPerfResultFileDelim = ',';

    StreamingBenchmark::StreamingBenchmark(std::string benchmarkName)
        : srcOpName_(std::string(benchmarkName).append("_src")),
          sinkOpName_(std::string(benchmarkName).append("_sink")), benchmarkName_(std::move(benchmarkName))
    {
    }

    StreamingBenchmark::~StreamingBenchmark()
    {
        for (const auto& streamingJob: benchmarkStreamingJobs_) {
            delete streamingJob;
        }
        delete executionEngine_;
    }

    void StreamingBenchmark::Initialize(unsigned int iteration, size_t maxMemory, int32_t numEventsPerBlock,
            size_t numBlocksPerChunk, int32_t backPressureThreshold, const std::string& schedulingModeStr,
            std::string systemIDString, uint64_t schedulingPeriodMs, uint32_t numWorkers,
            const std::string& processingModeStr, int numQueries)
    {
        iter_ = iteration;
        processingMode_ = GetProcessingMode(processingModeStr);
        schedulingMode_ = GetSchedulingMode(schedulingModeStr);
        pMemoryManager_ = new MemManT(maxMemory, numBlocksPerChunk, numEventsPerBlock, MemManT::AllocatorType::kBasic);
        pMemoryManager_->SetMaxActiveChunksPerOperator(backPressureThreshold);
        profilerPtr_ = new ProflierT(10, true, std::move(systemIDString));
        executionEngine_ = new EngineT;
        executionEngine_->SetSchedulingPeriodMs(schedulingPeriodMs);
        executionEngine_->SetPriorityType(priorityType_);
        executionEngine_->SetMaxIdleThresholdMs(maxIdleThresholdMs_);
        executionEngine_->Init(pMemoryManager_, profilerPtr_, schedulingMode_, numWorkers, processingMode_,
                preemptMode_);
        executionEngine_->Start();
        for (int i = 0; i < numQueries; i++) {
            benchmarkStreamingJobs_.emplace_back(new runtime::StreamingJob);
        }
    }

    std::vector<std::string> StreamingBenchmark::GetTargetValuesAsTokenizedVectorFromFile(const std::string& filename,
            enjima::metrics::Profiler* pProf, const std::string& targetVal, size_t targetIdx, size_t metricNameIdx)
    {
        pProf->FlushMetricsLogger();
        auto metricsLoggerThreadId = pProf->GetMetricsLoggerThreadId();
        auto threadIdStr = std::to_string(metricsLoggerThreadId);
        std::ifstream inputFileStream(filename);
        std::vector<std::string> matchedLines;
        std::string line;
        while (std::getline(inputFileStream, line)) {
            if (line.find(threadIdStr) != std::string::npos && line.find(targetVal) != std::string::npos) {
                matchedLines.push_back(line);
            }
        }
        std::vector<std::string> targetValueVec;
        std::vector<std::string> tokenizedStrVec;
        tokenizedStrVec.reserve(10);
        std::string delim = ",";
        size_t pos;
        std::string token;
        for (auto& matchedLine: matchedLines) {
            tokenizedStrVec.clear();
            while ((pos = matchedLine.find(delim)) != std::string::npos) {
                token = matchedLine.substr(0, pos);
                tokenizedStrVec.push_back(token);
                matchedLine.erase(0, pos + delim.length());
            }
            tokenizedStrVec.push_back(matchedLine);
            if (tokenizedStrVec.at(metricNameIdx) == targetVal) {
                targetValueVec.push_back(tokenizedStrVec.at(targetIdx));
            }
        }
        return targetValueVec;
    }

    void StreamingBenchmark::PrintLatencyResults()
    {
        double overallAvgLatencySum = 0;
        for (const auto& queryId_: queryIds_) {
            auto queryIdSuffix = std::to_string(queryId_.GetId());
            auto targetMetricName = GetSuffixedOperatorName(sinkOpName_, queryIdSuffix).append("_latency_histogram");
            auto avgLatencyValues = GetTargetValuesAsTokenizedVectorFromFile("metrics/latency.csv", profilerPtr_,
                    targetMetricName, 4, 3);
            if (!avgLatencyValues.empty()) {
                uint64_t avgLatencySumForQ = 0;
                uint64_t avgLatencyCountForQ = 0;
                for (size_t i = 1; i < (avgLatencyValues.size() - 1); i++) {
                    avgLatencySumForQ += std::stoul(avgLatencyValues.at(i));
                    avgLatencyCountForQ++;
                }
                double overallAvgLatencyForQ =
                        static_cast<double>(avgLatencySumForQ) / static_cast<double>(avgLatencyCountForQ);
                overallAvgLatencySum += overallAvgLatencyForQ;
                std::cout << "Avg. latency average for " << targetMetricName << " : " << overallAvgLatencyForQ
                          << std::endl;
            }
        }
        std::cout << std::endl
                  << "Avg. latency average for " << queryIds_.size()
                  << " queries : " << static_cast<double>(overallAvgLatencySum) / static_cast<double>(queryIds_.size())
                  << std::endl;
        std::cout << std::endl;
    }

    void StreamingBenchmark::PrintThroughputResults()
    {
        std::cout << std::fixed << std::setprecision(2) << "Setting 'cout' format to 'fixed' and precision to '2'"
                  << std::endl;
        double overallTpSum = 0;
        for (const auto& queryId_: queryIds_) {
            auto queryIdSuffix = std::to_string(queryId_.GetId());
            auto targetMetricName = GetSuffixedOperatorName(srcOpName_, queryIdSuffix).append("_outThroughput_gauge");
            auto tpValues = GetTargetValuesAsTokenizedVectorFromFile("metrics/throughput.csv", profilerPtr_,
                    targetMetricName, 4, 3);
            if (!tpValues.empty()) {
                double tpSumForQ = 0;
                uint64_t tpCountForQ = 0;
                // We discard the first and the last value
                for (size_t i = 1; i < (tpValues.size() - 1); i++) {
                    tpSumForQ += std::stod(tpValues.at(i));
                    tpCountForQ++;
                }
                double avgTpForQ = tpSumForQ / static_cast<double>(tpCountForQ);
                overallTpSum += avgTpForQ;
                std::cout << "Avg. throughput for " << targetMetricName << " : " << avgTpForQ << std::endl;
            }
        }
        std::cout << std::endl
                  << "Sum throughput for " << queryIds_.size() << " queries : " << overallTpSum << std::endl;
        std::cout << std::endl;
    }

    void StreamingBenchmark::StopBenchmark()
    {
        try {
            PrintAndWriteSummaryResults();
            PrintThroughputResults();
            PrintLatencyResults();
        }
        catch (std::exception& e) {
            std::cout << "Exception when summarizing results: " << e.what() << std::endl;
        }
        executionEngine_->Shutdown();
    }

    void StreamingBenchmark::PrintAndWriteSummaryResults()
    {
        uint64_t totSrcOutCount = 0;
        double sumLastLatencyVals = 0;
        for (const auto& queryId_: queryIds_) {
            auto queryIdSuffix = std::to_string(queryId_.GetId());
            auto suffixedSrcOpName = GetSuffixedOperatorName(srcOpName_, queryIdSuffix);
            auto srcOutCounterMetricName = suffixedSrcOpName + metrics::kOutCounterSuffix;
            std::unsigned_integral auto srcOutCount = profilerPtr_->GetCounter(srcOutCounterMetricName)->GetCount();
            std::cout << "Output count for " << suffixedSrcOpName << " : " << srcOutCount << std::endl;
            totSrcOutCount += srcOutCount;
            auto suffixedSinkOpName = GetSuffixedOperatorName(sinkOpName_, queryIdSuffix);
            auto sinkLastAvgLatency = profilerPtr_->GetLatencyHistogram(suffixedSinkOpName)->GetAverage();
            sumLastLatencyVals += sinkLastAvgLatency;
        }

        auto timeDurationSec = std::chrono::duration_cast<std::chrono::seconds>(endTime_ - startTime_);
        auto throughput = (double) totSrcOutCount / (double) timeDurationSec.count();
        std::cout << "Benchmark (" << benchmarkName_ << ") ran for " << timeDurationSec.count()
                  << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughput
                  << " events/sec, Last Avg. Latency Measurement: "
                  << sumLastLatencyVals / static_cast<double>(queryIds_.size()) << " units]" << std::endl;
        writeResultToFile(totSrcOutCount, timeDurationSec, throughput);
        std::cout << "Wrote results to " << kPerfResultFilename << std::endl;
        std::cout << std::endl;
    }

    void StreamingBenchmark::writeResultToFile(uint64_t srcOutCount, const std::chrono::seconds& timeDurationSec,
            double throughput) const
    {
        std::ofstream perfResultFile;
        perfResultFile.open("metrics/" + kPerfResultFilename, std::ios::app);
        perfResultFile << benchmarkName_ << kPerfResultFileDelim << iter_ << kPerfResultFileDelim
                       << startTime_.time_since_epoch().count() << kPerfResultFileDelim << timeDurationSec.count()
                       << kPerfResultFileDelim << srcOutCount << kPerfResultFileDelim << std::fixed
                       << std::setprecision(2) << throughput << std::endl;
        perfResultFile.close();
    }

    const std::string& StreamingBenchmark::GetBenchmarkName() const
    {
        return benchmarkName_;
    }

    runtime::StreamingTask::ProcessingMode StreamingBenchmark::GetProcessingMode(const std::string& processingModeStr)
    {
        if (processingModeStr == "QueueBasedSingle") {
            return runtime::StreamingTask::ProcessingMode::kQueueBasedSingle;
        }
        else if (processingModeStr == "BlockBasedSingle") {
            return runtime::StreamingTask::ProcessingMode::kBlockBasedSingle;
        }
        else if (processingModeStr == "BlockBasedBatch") {
            return runtime::StreamingTask::ProcessingMode::kBlockBasedBatch;
        }
        throw WorkloadException(std::string("Invalid processing mode : ").append(processingModeStr));
    }

    runtime::SchedulingMode StreamingBenchmark::GetSchedulingMode(const std::string& schedulingModeStr)
    {
        if (schedulingModeStr == "SBPriority") {
            return runtime::SchedulingMode::kStateBasedPriority;
        }
        else if (schedulingModeStr == "TB") {
            return runtime::SchedulingMode::kThreadBased;
        }
        throw WorkloadException(std::string("Invalid scheduling mode : ").append(schedulingModeStr));
    }

    runtime::PriorityType StreamingBenchmark::GetPriorityType(const std::string& priorityTypeStr)
    {
        if (priorityTypeStr == "InputQueueSize") {
            return runtime::PriorityType::kInputQueueSize;
        }
        else if (priorityTypeStr == "Adaptive") {
            return runtime::PriorityType::kAdaptive;
        }
        else if (priorityTypeStr == "LatencyOptimized") {
            return runtime::PriorityType::kLatencyOptimized;
        }
        else if (priorityTypeStr == "SPLatencyOptimized") {
            return runtime::PriorityType::kSPLatencyOptimized;
        }
        else if (priorityTypeStr == "SimpleLatency") {
            return runtime::PriorityType::kSimpleLatency;
        }
        else if (priorityTypeStr == "LeastRecent") {
            return runtime::PriorityType::kLeastRecent;
        }
        else if (priorityTypeStr == "ThroughputOptimized") {
            return runtime::PriorityType::kThroughputOptimized;
        }
        else if (priorityTypeStr == "SimpleThroughput") {
            return runtime::PriorityType::kSimpleThroughput;
        }
        else if (priorityTypeStr == "RoundRobin") {
            return runtime::PriorityType::kRoundRobin;
        }
        else if (priorityTypeStr == "FCFS") {
            return runtime::PriorityType::kFirstComeFirstServed;
        }
        throw WorkloadException(std::string("Invalid priority type : ").append(priorityTypeStr));
    }

    runtime::PreemptMode StreamingBenchmark::GetPreemptMode(const std::string& preemptModeStr)
    {
        if (preemptModeStr == "Preemptive") {
            return runtime::PreemptMode::kPreemptive;
        }
        else if (preemptModeStr == "NonPreemptive") {
            return runtime::PreemptMode::kNonPreemptive;
        }
        throw WorkloadException(std::string("Invalid preempt mode : ").append(preemptModeStr));
    }

    void StreamingBenchmark::SetPreemptMode(const std::string& preemptModeStr)
    {
        preemptMode_ = GetPreemptMode(preemptModeStr);
    }

    void StreamingBenchmark::SetPriorityType(const std::string& priorityTypeStr)
    {
        priorityType_ = GetPriorityType(priorityTypeStr);
    }

    std::string StreamingBenchmark::GetSuffixedOperatorName(const std::string& opName, const std::string& jobIdSuffix)
    {
        return std::string(opName).append("_").append(jobIdSuffix);
    }

    void StreamingBenchmark::SetMaxIdleThresholdMs(uint64_t maxIdleThresholdMs)
    {
        maxIdleThresholdMs_ = maxIdleThresholdMs;
    }

    void StreamingBenchmark::SetSourceReservoirCapacity(uint64_t srcReservoirCapacity)
    {
        srcReservoirCapacity_ = srcReservoirCapacity;
    }

}// namespace enjima::benchmarks::workload