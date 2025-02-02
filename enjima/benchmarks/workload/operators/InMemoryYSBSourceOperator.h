//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_YSB_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_YSB_SOURCE_OPERATOR_H


#include "enjima/api/data_types/YSBAdEvent.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include <random>

using YSBAdT = enjima::api::data_types::YSBAdEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryYSBSourceOperator : public enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration> {
    public:
        explicit InMemoryYSBSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs)
            : enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs)
        {
        }

        bool EmitEvent(core::OutputCollector* collector) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                auto latencyRecord = enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>());
                collector->Collect(latencyRecord);
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                }
                auto ysbEvent = *cacheIterator_.base();
                collector->Collect<YSBAdT>(ysbEvent);
            }
            return true;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer, core::OutputCollector* collector) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            void* outBufferWritePtr = outputBuffer;
            uint32_t numLatencyRecordsOut = 0;
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                new (outBufferWritePtr) enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>());
                outBufferWritePtr = static_cast<enjima::core::Record<YSBAdT>*>(outBufferWritePtr) + 1;
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }
            for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                }
                auto ysbEvent = *cacheIterator_.base();
                new (outBufferWritePtr) enjima::core::Record<YSBAdT>(currentTime, ysbEvent);
                outBufferWritePtr = static_cast<enjima::core::Record<YSBAdT>*>(outBufferWritePtr) + 1;
            }
            collector->CollectBatch<YSBAdT>(outputBuffer, maxRecordsToWrite);
            return maxRecordsToWrite;
        }

        YSBAdT GenerateQueueEvent() override
        {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto ysbEvent = *cacheIterator_.base();
            return ysbEvent;
        }

        bool GenerateQueueRecord(core::Record<YSBAdT>& outputRecord) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                outputRecord = enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>());
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                auto event = GenerateQueueEvent();
                outputRecord = enjima::core::Record<YSBAdT>(currentTime, event);
            }
            return true;
        }

        void PopulateEventCache(uint32_t numEvents, uint32_t numCampaigns, uint32_t numAdsPerCampaign)
        {
            UniformIntDistParamT pt(1, 1000000);
            uniformIntDistribution.param(pt);
            std::unordered_set<uint64_t> campaignIds(numCampaigns);
            auto adId = 0;
            auto numAds = numCampaigns * numAdsPerCampaign;
            while (campaignIds.size() < numCampaigns) {
                auto campaignId = uniformIntDistribution(mtEng);
                if (!campaignIds.contains(campaignId)) {
                    campaignIds.emplace(campaignId);
                    for (auto j = numAdsPerCampaign; j > 0; j--) {
                        adIdToCampaignIdMap_.emplace(++adId, campaignId);
                    }
                }
            }

            eventCache_.reserve(numEvents);
            for (auto i = numEvents; i > 0; i--) {
                auto timestamp = enjima::runtime::GetSystemTimeMillis();
                auto userId = uniformIntDistribution(mtEng);
                auto pageId = uniformIntDistribution(mtEng);
                auto adType = i % 5;
                auto eventType = i % 3;
                YSBAdT lrEvent = YSBAdT(timestamp, userId, pageId, (i % numAds) + 1, adType, eventType, -1);
                eventCache_.emplace_back(lrEvent);
            }
            cacheIterator_ = eventCache_.cbegin();
        }

        const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
        {
            return adIdToCampaignIdMap_;
        }

    private:
        std::random_device rd;
        std::mt19937 mtEng{rd()};
        std::uniform_int_distribution<int> uniformIntDistribution;
        std::vector<YSBAdT> eventCache_;
        std::vector<YSBAdT>::const_iterator cacheIterator_;
        UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_YSB_SOURCE_OPERATOR_H
