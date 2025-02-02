//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_LINEAR_ROAD_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_LINEAR_ROAD_SOURCE_OPERATOR_H


#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/operators/SourceOperator.h"
#include <random>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

class InMemoryLRBSourceOperator : public enjima::operators::SourceOperator<LinearRoadT> {
public:
    explicit InMemoryLRBSourceOperator(enjima::operators::OperatorID opId, const std::string& opName)
        : enjima::operators::SourceOperator<LinearRoadT>(opId, opName)
    {
    }


    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto lrEvent = *cacheIterator_.base();
        collector->Collect<LinearRoadT>(lrEvent);
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        for (auto i = maxRecordsToWrite; i > 0; i--) {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto lrEvent = *cacheIterator_.base();
            new (outBufferWritePtr) enjima::core::Record<LinearRoadT>(currentTime, lrEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<LinearRoadT>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<LinearRoadT>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    void PopulateEventCache(uint32_t numEventsInCache)
    {
        eventCache_.reserve(numEventsInCache);
        for (auto i = numEventsInCache; i > 0; i--) {
            LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                    GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                    GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
            eventCache_.emplace_back(lrEvent);
        }
        cacheIterator_ = eventCache_.cbegin();
    }

    LinearRoadT GenerateQueueEvent() override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto lrEvent = *cacheIterator_.base();
        return lrEvent;
    }

    bool GenerateQueueRecord(enjima::core::Record<LinearRoadT>& outputRecord) override
    {
        LinearRoadT event = InMemoryLRBSourceOperator::GenerateQueueEvent();
        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
        outputRecord = enjima::core::Record<LinearRoadT>(currentSysTime, event);
        return true;
    }

private:
    int GetRandomInt(const int& a, const int& b)
    {
        UniformIntDistParamT pt(a, b);
        uniformIntDistribution.param(pt);
        return uniformIntDistribution(gen);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::vector<LinearRoadT> eventCache_;
    std::vector<LinearRoadT>::const_iterator cacheIterator_;
};


#endif//ENJIMA_BENCHMARKS_IN_MEMORY_LINEAR_ROAD_SOURCE_OPERATOR_H
