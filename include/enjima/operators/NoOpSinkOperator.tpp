//
// Created by m34ferna on 05/02/24.
//

namespace enjima::operators {
    template<typename TInput>
    NoOpSinkOperator<TInput>::NoOpSinkOperator(OperatorID opId, const std::string& opName)
        : SinkOperator<TInput>(opId, opName)
    {
    }

    template<typename TInput>
    size_t NoOpSinkOperator<TInput>::GetOutputRecordSize() const
    {
        return 0;
    }

    template<typename TInput>
    void NoOpSinkOperator<TInput>::ProcessSinkEvent(uint64_t timestamp, TInput inputEvent)
    {
    }

    template<typename TInput>
    void NoOpSinkOperator<TInput>::ProcessSinkBatch(void* inputBuffer, uint32_t numRecordsToRead)
    {
        if (numRecordsToRead > 0) {
            core::Record<TInput>* pInputRecord;
            for (auto i = numRecordsToRead; i > 0; i--) {
                pInputRecord = static_cast<core::Record<TInput>*>(inputBuffer);
                if (pInputRecord->GetRecordType() == core::Record<TInput>::RecordType::kLatency) {
#if ENJIMA_METRICS_LEVEL >= 3
                    auto metricsVecPtr = pInputRecord->GetAdditionalMetricsPtr();
                    metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                    this->latencyHistogram_->AddAdditionalMetrics(metricsVecPtr);
#endif
                    auto currentTimestamp = enjima::runtime::GetSystemTimeMillis();
                    this->latencyHistogram_->Update(currentTimestamp - pInputRecord->GetTimestamp());
                }
                inputBuffer = static_cast<core::Record<TInput>*>(inputBuffer) + 1;
            }
        }
    }

}// namespace enjima::operators
