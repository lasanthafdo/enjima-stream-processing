#include "cxxopts/cxxopts.hpp"
#include "enjima/benchmarks/workload/LRBValidateBenchmark.h"
#include "enjima/benchmarks/workload/LinearRoadBenchmark.h"
#include "enjima/benchmarks/workload/NewYorkTaxiBenchmark.h"
#include "enjima/benchmarks/workload/NewYorkTaxiDQBenchmark.h"
#include "enjima/benchmarks/workload/NewYorkTaxiOptimizedDQBenchmark.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/benchmarks/workload/YSBSourceBenchmark.h"
#include "enjima/benchmarks/workload/YahooStreamingBenchmark.h"
#include "enjima/memory/MemoryUtil.h"
#include "yaml-cpp/yaml.h"

#include <filesystem>
#include <iostream>
#include <set>

using namespace enjima::benchmarks::workload;

long GetProgramDir(char* pBuf, ssize_t len);
YAML::Node GetYAMLConfig();

int main(int argc, const char** argv)
{
    cxxopts::Options options("enjima_benchmarks", "A benchmark suite for Enjima");

    options.add_options("default",
            {{"d,debug", "Enable debugging", cxxopts::value<bool>()->default_value("false")},
                    {"b,bench", "Benchmark name", cxxopts::value<std::string>()->default_value("LRB")},
                    {"s,scheduling_mode", "Operator scheduling mode",
                            cxxopts::value<std::string>()->default_value("TB")},
                    {"scheduling_period", "Scheduling period (in milliseconds)",
                            cxxopts::value<uint64_t>()->default_value("50")},
                    {"t,duration", "Benchmark duration (in seconds)", cxxopts::value<uint64_t>()->default_value("30")},
                    {"i, rate", "Input event rate (in events/seconds)",
                            cxxopts::value<uint64_t>()->default_value("1000000")},
                    {"r,repeat", "Number of times to repeat", cxxopts::value<int>()->default_value("1")},
                    {"q,num_queries", "Number of queries to execute", cxxopts::value<int>()->default_value("1")},
                    {"w,num_workers", "Number of worker threads to spawn for state-based scheduling",
                            cxxopts::value<uint32_t>()->default_value("8")},
                    {"l,latency_record_period",
                            "A latency record will be emitted from the source operator this number of milliseconds "
                            "after the previous",
                            cxxopts::value<uint64_t>()->default_value("50")},
                    {"latency_res", "Resolution of the latency timestamp (e.g., milliseconds, microseconds etc.)",
                            cxxopts::value<std::string>()->default_value("Millis")},
                    {"m,memory", "Amount of memory for data exchange (in megabytes)",
                            cxxopts::value<size_t>()->default_value("64")},
                    {"events_per_block", "Number of events per memory block",
                            cxxopts::value<int32_t>()->default_value("384")},
                    {"blocks_per_chunk", "Number of blocks per memory chunk",
                            cxxopts::value<size_t>()->default_value("4")},
                    {"backpressure_threshold",
                            "Number of chunks that can be active per operator before backpressure is triggered",
                            cxxopts::value<int32_t>()->default_value("100")},
                    {"max_idle_threshold_ms",
                            "Maximum time (ms) an operator is allowed to be idle before being rescheduled",
                            cxxopts::value<uint64_t>()->default_value("10")},
                    {"src_reservoir_size", "Capacity of the event reservoir at the generator",
                            cxxopts::value<uint64_t>()->default_value("10000000")},
                    {"p,processing_mode", "Processing mode",
                            cxxopts::value<std::string>()->default_value("BlockBasedBatch")},
                    {"preempt_mode", "Preempt mode for scheduling",
                            cxxopts::value<std::string>()->default_value("NonPreemptive")},
                    {"priority_type", "Priority calculation type for scheduling",
                            cxxopts::value<std::string>()->default_value("InputQueueSize")},
                    {"generate_with_emit", "Enable generating events at the time of emission by the source operator",
                            cxxopts::value<bool>()->default_value("false")},
                    {"use_processing_latency",
                            "Latency markers are generated at the time of ingestion by the source operator",
                            cxxopts::value<bool>()->default_value("false")},
                    {"data_path", "Path to source files generating events",
                            cxxopts::value<std::string>()->default_value("./car.dat")},
                    {"h,help", "Print usage"}});

    auto result = options.parse(argc, argv);

    if (result.count("bench")) {
        std::cout << "Starting benchmark suite for Enjima..." << std::endl;
        auto benchmark = result["bench"].as<std::string>();
        auto benchDurationSecs = result["duration"].as<uint64_t>();
        auto maxInputRate = result["rate"].as<uint64_t>();
        auto latencyRecEmitPeriodMs = result["latency_record_period"].as<uint64_t>();
        auto latencyRecResolution = result["latency_res"].as<std::string>();
        auto processingModeStr = result["processing_mode"].as<std::string>();
        auto schedulingPeriodMs = result["scheduling_period"].as<uint64_t>();
        auto schedulingModeStr = result["scheduling_mode"].as<std::string>();
        auto preemptModeStr = result["preempt_mode"].as<std::string>();
        auto priorityTypeStr = result["priority_type"].as<std::string>();
        auto maxMemoryMb = result["memory"].as<size_t>();
        auto numEventsPerBlock = result["events_per_block"].as<int32_t>();
        auto numBlocksPerChunk = result["blocks_per_chunk"].as<size_t>();
        auto numRepeats = result["repeat"].as<int>();
        auto numQueries = result["num_queries"].as<int>();
        auto numWorkers = result["num_workers"].as<uint32_t>();
        auto backPressureThreshold = result["backpressure_threshold"].as<int32_t>();
        auto maxIdleThresholdMs = result["max_idle_threshold_ms"].as<uint64_t>();
        auto srcReservoirCapacity = result["src_reservoir_size"].as<uint64_t>();
        auto genWithEmit = result["generate_with_emit"].as<bool>();
        auto useProcessingLatency = result["use_processing_latency"].as<bool>();
        auto dataPath = result["data_path"].as<std::string>();
        auto memAllocStrategy = "PreAllocate";

        YAML::Node yamlConfig = GetYAMLConfig();
        auto preAllocMode = yamlConfig["runtime"]["preAllocationMode"].as<uint8_t>();
        if (preAllocMode == 0 || (preAllocMode == 2 && schedulingModeStr == "TB") ||
                (preAllocMode == 2 && schedulingModeStr == "SBPriority" &&
                        (priorityTypeStr != "LatencyOptimized" || processingModeStr != "BlockBasedBatch"))) {
            memAllocStrategy = "OnDemand";
        }

        std::set<std::string> validBenchmarks{"YSB", "LRB", "YSBSB", "LRBVB", "NYT", "NYTDQ", "NYTODQ"};
        if (!validBenchmarks.contains(benchmark)) {
            throw WorkloadException("Invalid benchmark name: " + benchmark);
        }
        for (auto i = 0; i < numRepeats; i++) {
            if (i > 0) {
                std::cout << "Sleeping for 5 seconds before starting next iteration ..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(5));
                std::cout << std::endl;
            }
            auto iter = i + 1;
            std::string systemIDString = std::string("Enjima_")
                                                 .append(benchmark)
                                                 .append("_")
                                                 .append(std::to_string(maxInputRate))
                                                 .append("_")
                                                 .append(processingModeStr)
                                                 .append("_")
                                                 .append(schedulingModeStr)
                                                 .append("_")
                                                 .append(std::to_string(iter))
                                                 .append("_")
                                                 .append(preemptModeStr)
                                                 .append("_")
                                                 .append(priorityTypeStr)
                                                 .append("_")
                                                 .append(std::to_string(numWorkers))
                                                 .append("_")
                                                 .append(std::to_string(maxMemoryMb))
                                                 .append("_")
                                                 .append(std::to_string(backPressureThreshold))
                                                 .append("_")
                                                 .append(latencyRecResolution)
                                                 .append("_")
                                                 .append(std::to_string(schedulingPeriodMs))
                                                 .append("_")
                                                 .append(std::to_string(numQueries))
                                                 .append("_")
                                                 .append(memAllocStrategy)
                                                 .append("_")
                                                 .append(std::to_string(numEventsPerBlock))
                                                 .append("_")
                                                 .append(std::to_string(numBlocksPerChunk));
            StreamingBenchmark* benchmarkPtr;
            std::cout << "Running " << benchmark << " benchmark iteration " << iter
                      << " with system ID: " << systemIDString << std::endl;
            if (benchmark == "YSB") {
                if (latencyRecResolution == "Micros") {
                    benchmarkPtr = new YahooStreamingBenchmark<std::chrono::microseconds>;
                }
                else if (latencyRecResolution == "Millis") {
                    benchmarkPtr = new YahooStreamingBenchmark;
                }
                else {
                    throw WorkloadException("Invalid resolution for latency record timestamps!");
                }
            }
            else if (benchmark == "YSBSB") {
                if (latencyRecResolution == "Micros") {
                    benchmarkPtr = new YSBSourceBenchmark<std::chrono::microseconds>;
                }
                else if (latencyRecResolution == "Millis") {
                    benchmarkPtr = new YSBSourceBenchmark;
                }
                else {
                    throw WorkloadException("Invalid resolution for latency record timestamps!");
                }
            }
            else if (benchmark == "LRB") {
                if (latencyRecResolution == "Micros") {
                    benchmarkPtr = new LinearRoadBenchmark<std::chrono::microseconds>;
                    dynamic_cast<LinearRoadBenchmark<std::chrono::microseconds>*>(benchmarkPtr)->SetDataPath(dataPath);
                }
                else if (latencyRecResolution == "Millis") {
                    benchmarkPtr = new LinearRoadBenchmark;
                    dynamic_cast<LinearRoadBenchmark<std::chrono::milliseconds>*>(benchmarkPtr)->SetDataPath(dataPath);
                }
                else {
                    throw WorkloadException("Invalid resolution for latency record timestamps!");
                }
            }
            else if (benchmark == "LRBVB") {
                benchmarkPtr = new LRBValidateBenchmark;
                dynamic_cast<LRBValidateBenchmark*>(benchmarkPtr)->SetDataPath(dataPath);
            }
            else if (benchmark == "NYT") {
                if (latencyRecResolution == "Micros") {
                    benchmarkPtr = new NewYorkTaxiBenchmark<std::chrono::microseconds>;
                    dynamic_cast<NewYorkTaxiBenchmark<std::chrono::microseconds>*>(benchmarkPtr)->SetDataPath(dataPath);
                }
                else if (latencyRecResolution == "Millis") {
                    benchmarkPtr = new NewYorkTaxiBenchmark;
                    dynamic_cast<NewYorkTaxiBenchmark<std::chrono::milliseconds>*>(benchmarkPtr)->SetDataPath(dataPath);
                }
                else {
                    throw WorkloadException("Invalid resolution for latency record timestamps!");
                }
            }
            else if (benchmark == "NYTDQ") {
                if (latencyRecResolution == "Micros") {
                    benchmarkPtr = new NewYorkTaxiDQBenchmark<std::chrono::microseconds>;
                    dynamic_cast<NewYorkTaxiDQBenchmark<std::chrono::microseconds>*>(benchmarkPtr)
                            ->SetDataPath(dataPath);
                }
                else if (latencyRecResolution == "Millis") {
                    benchmarkPtr = new NewYorkTaxiDQBenchmark;
                    dynamic_cast<NewYorkTaxiDQBenchmark<std::chrono::milliseconds>*>(benchmarkPtr)
                            ->SetDataPath(dataPath);
                }
                else {
                    throw WorkloadException("Invalid resolution for latency record timestamps!");
                }
            }
            else if (benchmark == "NYTODQ") {
                if (latencyRecResolution == "Micros") {
                    benchmarkPtr = new NewYorkTaxiOptimizedDQBenchmark<std::chrono::microseconds>;
                    dynamic_cast<NewYorkTaxiOptimizedDQBenchmark<std::chrono::microseconds>*>(benchmarkPtr)
                            ->SetDataPath(dataPath);
                }
                else if (latencyRecResolution == "Millis") {
                    benchmarkPtr = new NewYorkTaxiOptimizedDQBenchmark;
                    dynamic_cast<NewYorkTaxiOptimizedDQBenchmark<std::chrono::milliseconds>*>(benchmarkPtr)
                            ->SetDataPath(dataPath);
                }
                else {
                    throw WorkloadException("Invalid resolution for latency record timestamps!");
                }
            }
            else {
                throw WorkloadException("Invalid benchmark name specified!");
            }
            benchmarkPtr->SetPreemptMode(preemptModeStr);
            benchmarkPtr->SetPriorityType(priorityTypeStr);
            benchmarkPtr->SetMaxIdleThresholdMs(maxIdleThresholdMs);
            benchmarkPtr->SetSourceReservoirCapacity(srcReservoirCapacity);
            benchmarkPtr->Initialize(iter, enjima::memory::MegaBytes(maxMemoryMb), numEventsPerBlock, numBlocksPerChunk,
                    backPressureThreshold, schedulingModeStr, systemIDString, schedulingPeriodMs, numWorkers,
                    processingModeStr, numQueries);
            benchmarkPtr->SetUpPipeline(latencyRecEmitPeriodMs, maxInputRate, genWithEmit, useProcessingLatency);
            benchmarkPtr->RunBenchmark(benchDurationSecs);
            benchmarkPtr->StopBenchmark();
            std::cout << "Finished YSB benchmark iteration " << iter << std::endl;
            delete benchmarkPtr;
        }
        return 0;
    }

    std::cout << options.help() << std::endl;
    return 0;
}

YAML::Node GetYAMLConfig()
{
    std::string configFilePath = "conf/enjima-config.yaml";
    YAML::Node yamlConfig;
    try {
        std::filesystem::path fsPathConfig{configFilePath};
        if (!std::filesystem::exists(fsPathConfig)) {
            char programPathBuf[256];
            if (GetProgramDir(programPathBuf, sizeof(programPathBuf)) > 0) {
                std::filesystem::path programPath{std::string(programPathBuf)};
                fsPathConfig = programPath.parent_path().append(configFilePath);
                configFilePath = fsPathConfig.string();
            }
        }
        yamlConfig = YAML::LoadFile(configFilePath);
    }
    catch (YAML::BadFile& ex) {
        spdlog::error("Error when loading configuration file from " + configFilePath);
        throw WorkloadException{std::string("Error loading configuration file: ").append(ex.what())};
    }
    return yamlConfig;
}

long GetProgramDir(char* pBuf, ssize_t len)
{
    auto bytes = std::min(readlink("/proc/self/exe", pBuf, len), len - 1);
    if (bytes >= 0) pBuf[bytes] = '\0';
    return bytes;
}
