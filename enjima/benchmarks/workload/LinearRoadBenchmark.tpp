//
// Created by m34ferna on 21/02/24.
//

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

template<typename Duration>
using LRBSrcOpT = enjima::workload::operators::InMemoryRateLimitedLRBSourceOperator<Duration>;
template<typename Duration>
using FixedRateLRBSrcOpT = enjima::workload::operators::InMemoryFixedRateLRBSourceOperator<Duration>;
template<typename Duration>
using FixedRateLRBSrcOpWithProcLatencyT =
        enjima::workload::operators::InMemoryFixedRateLRBSourceOperatorWithProcLatency<Duration>;

using LRBProjFuncT = enjima::workload::functions::LRBEventProjectFunction;
using LRBCountAggFuncT = enjima::workload::functions::LRBCountAggFunction;
using LRBSpeedAggFuncT = enjima::workload::functions::LRBSpeedAggFunction;
using LRBCntMultiKeyExtractFuncT = enjima::workload::functions::LRBCntReportMultiKeyExtractFunction;
using LRBSpdMultiKeyExtractFuncT = enjima::workload::functions::LRBSpdReportMultiKeyExtractFunction;
using LRBSpdCntJoinFuncT = enjima::workload::functions::LRBSpdCntJoinFunction;
using LRBTollAcdCoGroupFuncT = enjima::workload::functions::LRBTollAcdCoGroupFunction;

namespace enjima::benchmarks::workload {
    template<typename Duration>
    void LinearRoadBenchmark<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        auto jobIdSuffix = 0;
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (generateWithEmit) {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<LRBSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
                uPtrSrcOp->PopulateEventCache(lrbDataPath_);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else {
                if (useProcessingLatency) {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<FixedRateLRBSrcOpWithProcLatencyT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                            srcReservoirCapacity_);
                    uPtrSrcOp->PopulateEventCache(lrbDataPath_, &eventCache_);
                    SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<FixedRateLRBSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                            srcReservoirCapacity_);
                    uPtrSrcOp->PopulateEventCache(lrbDataPath_, &eventCache_);
                    SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
            }
            if (schedulingMode_ == runtime::SchedulingMode::kThreadBased) {
                benchmarkStreamingJob->SetProcessingMode(processingMode_);
            }
        }
    }

    template<typename Duration>
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingLRBSrcOpT<Duration>, T>
    void LinearRoadBenchmark<Duration>::SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Filtering position reports
        enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
        auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetType() == 0; };
        auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(
                filterOpId, GetSuffixedOperatorName(filterOpName_, jobIdSuffixStr), filterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), srcOpId);

        // Project
        // You can split into 3 here and avoid an extra "Identity" operator
        enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
        auto uPtrProjOp = std::make_unique<enjima::operators::MapOperator<LinearRoadT, LRBProjEventT, LRBProjFuncT, 3>>(
                projectOpId, GetSuffixedOperatorName(projOpName_, jobIdSuffixStr), LRBProjFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjOp), filterOpId);

        // Accident tracking: upstream at multiMapOpId
        enjima::operators::OperatorID acdFilterOpId = executionEngine_->GetNextOperatorId();
        auto acdFilterFn = [](const LRBProjEventT& lrInputEvent) {
            return (1 <= lrInputEvent.GetLane()) && (lrInputEvent.GetLane() <= 3);
        };
        auto uPtrAcdFilterOp =
                std::make_unique<enjima::operators::FilterOperator<LRBProjEventT, decltype(acdFilterFn)>>(acdFilterOpId,
                        GetSuffixedOperatorName(acdFilterOpName_, jobIdSuffixStr), acdFilterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrAcdFilterOp), projectOpId);

        enjima::operators::OperatorID windowOpAcdId = executionEngine_->GetNextOperatorId();
        auto acdAggfunc = enjima::api::MergeableKeyedAggregateFunction<LRBProjEventT, LRBAcdReportT>{};
        auto uPtrWindowOpAcd = std::make_unique<
                enjima::operators::SlidingEventTimeWindowOperator<LRBProjEventT, LRBAcdReportT, decltype(acdAggfunc)>>(
                windowOpAcdId, GetSuffixedOperatorName(acdWindowOpName_, jobIdSuffixStr), acdAggfunc,
                std::chrono::seconds(120), std::chrono::seconds(30));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpAcd), acdFilterOpId);

        // Speed monitor: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpSpdId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpSpd = std::make_unique<
                enjima::operators::FixedEventTimeWindowOperator<LRBProjEventT, LRBSpdReportT, true, LRBSpeedAggFuncT>>(
                windowOpSpdId, GetSuffixedOperatorName(spdWindowOpName_, jobIdSuffixStr), LRBSpeedAggFuncT{},
                std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpSpd), projectOpId);

        // Count vehicles: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpCntId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpCnt = std::make_unique<
                enjima::operators::FixedEventTimeWindowOperator<LRBProjEventT, LRBCntReportT, true, LRBCountAggFuncT>>(
                windowOpCntId, GetSuffixedOperatorName(cntWindowOpName_, jobIdSuffixStr), LRBCountAggFuncT{},
                std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpCnt), projectOpId);

        // Join Count and Speed
        enjima::operators::OperatorID windowJoinCntSpdId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinCntSpd = std::make_unique<enjima::operators::FixedEventTimeWindowJoinOperator<LRBSpdReportT,
                LRBCntReportT, std::tuple<int, int, int, uint64_t>, LRBSpdMultiKeyExtractFuncT,
                LRBCntMultiKeyExtractFuncT, LRBTollReportT, LRBSpdCntJoinFuncT>>(windowJoinCntSpdId,
                GetSuffixedOperatorName(cntSpdWindowJoinOpName_, jobIdSuffixStr), LRBSpdMultiKeyExtractFuncT{},
                LRBCntMultiKeyExtractFuncT{}, LRBSpdCntJoinFuncT{}, std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinCntSpd),
                std::make_pair(windowOpSpdId, windowOpCntId));

        // Join above with accident
        enjima::operators::OperatorID windowCoGroupOpId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinTollAcd =
                std::make_unique<enjima::operators::FixedEventTimeWindowCoGroupOperator<LRBTollReportT, LRBAcdReportT,
                        LRBTollFinalReportT, LRBTollAcdCoGroupFuncT>>(windowCoGroupOpId,
                        GetSuffixedOperatorName(acdTollCoGroupOpName_, jobIdSuffixStr), LRBTollAcdCoGroupFuncT{},
                        std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinTollAcd),
                std::make_pair(windowJoinCntSpdId, windowOpAcdId));

        // Sink
        enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LRBTollFinalReportT>>(sinkOpId,
                GetSuffixedOperatorName(sinkOpName_, jobIdSuffixStr));
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowCoGroupOpId);
    }

    template<typename Duration>
    void LinearRoadBenchmark<Duration>::RunBenchmark(uint64_t durationInSec)
    {
        spdlog::info("Starting benchmark : {}", this->GetBenchmarkName());
        startTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            queryIds_.emplace_back(executionEngine_->Submit(*benchmarkStreamingJob));
        }
        std::this_thread::sleep_for(std::chrono::seconds(durationInSec));
        try {
            for (const auto& jobId: queryIds_) {
                executionEngine_->Cancel(jobId);
            }
        }
        catch (const enjima::runtime::CancellationException& e) {
            spdlog::error("Could not gracefully cancel job: {}", e.what());
        }

        endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
    }

    template<typename Duration>
    LinearRoadBenchmark<Duration>::LinearRoadBenchmark() : StreamingBenchmark("lrb")
    {
    }

    template<typename Duration>
    void LinearRoadBenchmark<Duration>::SetDataPath(const std::string& path)
    {
        lrbDataPath_ = path;
    }
}// namespace enjima::benchmarks::workload