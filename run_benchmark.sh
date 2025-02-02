#!/bin/bash

SETUP_FILE="bench.yaml"
CONF_FILE="conf/enjima-config.yaml"
METRICS_TARGET_DIR=$HOME/enjima/metrics
BENCHMARK=$1
DURATION_SEC=$2
NUM_REPETITIONS=$3
DIRECTIVES=$4

TARGET_DIR=$(yq e .benchmarkConfig.${BENCHMARK}.targetDir < $SETUP_FILE)
PROCESSING_MODES=$(yq e .benchmarkConfig.${BENCHMARK}.processingModes < $SETUP_FILE)
SCHEDULING_MODES=$(yq e .benchmarkConfig.${BENCHMARK}.schedulingModes < $SETUP_FILE)
LATENCY_VALUES=$(yq e .benchmarkConfig.${BENCHMARK}.latencyValues < $SETUP_FILE)
INPUT_RATES=$(yq e .benchmarkConfig.${BENCHMARK}.inputRates < $SETUP_FILE)
NUM_WORKERS=$(yq e .benchmarkConfig.${BENCHMARK}.numWorkers < $SETUP_FILE)
PREEMPT_MODES=$(yq e .benchmarkConfig.${BENCHMARK}.preemptModes < $SETUP_FILE)
PRIORITY_TYPES=$(yq e .benchmarkConfig.${BENCHMARK}.priorityTypes < $SETUP_FILE)
NUM_QUERIES_ARR=$(yq e .benchmarkConfig.${BENCHMARK}.numQueriesArr < $SETUP_FILE)
MAX_MEMORY=$(yq e .benchmarkConfig.${BENCHMARK}.maxMemory < $SETUP_FILE)
BACKPRESSURE_THRESHOLD=$(yq e .benchmarkConfig.${BENCHMARK}.backpressureThreshold < $SETUP_FILE)
EVENTS_PER_BLOCK=$(yq e .benchmarkConfig.${BENCHMARK}.eventsPerBlock < $SETUP_FILE)
BLOCKS_PER_CHUNK=$(yq e .benchmarkConfig.${BENCHMARK}.blocksPerChunk < $SETUP_FILE)
SCHEDULING_PERIOD=$(yq e .benchmarkConfig.${BENCHMARK}.schedulingPeriod < $SETUP_FILE)
LATENCY_RES=$(yq e .benchmarkConfig.${BENCHMARK}.latencyRes < $SETUP_FILE)
LATENCY_TYPE=$(yq e .benchmarkConfig.${BENCHMARK}.latencyType < $SETUP_FILE)
MAX_IDLE_THRESHOLD_MS=$(yq e .benchmarkConfig.${BENCHMARK}.maxIdleThresholdMs < $SETUP_FILE)
PATH_TO_DATA=$(yq e .benchmarkConfig.${BENCHMARK}.dataPath < $SETUP_FILE)
SRC_RESERVOIR_SIZE=$(yq e .benchmarkConfig.${BENCHMARK}.srcReservoirSize < $SETUP_FILE)
WORKER_CPU_LIST=$(yq e .runtime.workerCpuList < $CONF_FILE)

TB_PROCESSING_MODES=
SB_PROCESSING_MODES=
TB_BACKPRESSURE_THRESHOLD=
SB_BACKPRESSURE_THRESHOLD=
QUERY_SPECIFIC_NUM_WORKERS=
PRIORITY_TYPE="Unassigned"

EXP_TYPE=$(yq e .benchmarkConfig.${BENCHMARK}.expType < $SETUP_FILE)
USE_TIMEOUT=$(yq e .benchmarkConfig.${BENCHMARK}.useTimeout < $SETUP_FILE)

if [[ $EXP_TYPE == "config_specific" ]]; then
	TB_PROCESSING_MODES=$(yq e .benchmarkConfig.${BENCHMARK}.specific.TB.processingModes < $SETUP_FILE)
	SB_PROCESSING_MODES=$(yq e .benchmarkConfig.${BENCHMARK}.specific.SB.processingModes < $SETUP_FILE)
	TB_BACKPRESSURE_THRESHOLD=$(yq e .benchmarkConfig.${BENCHMARK}.specific.TB.backpressureThreshold < $SETUP_FILE)
	SB_BACKPRESSURE_THRESHOLD=$(yq e .benchmarkConfig.${BENCHMARK}.specific.SB.backpressureThreshold < $SETUP_FILE)
	QUERY_SPECIFIC_NUM_WORKERS=$(yq e .benchmarkConfig.${BENCHMARK}.specific.querySpecificNumWorkers < $SETUP_FILE)
fi

archive_metrics() 
{
	DIR_NAME=$START_DATE_TIME
	ARCHIVE_NAME="${DIR_NAME}.tar.gz"
	echo "Creating metrics archive $ARCHIVE_NAME"
	mkdir $DIR_NAME
	cp metrics/*.csv $DIR_NAME/
	cp metrics/*.txt $DIR_NAME/
	tar cvzf $ARCHIVE_NAME $DIR_NAME
	mv $ARCHIVE_NAME $METRICS_TARGET_DIR
	rm -r $DIR_NAME 
}

execute_run_command()
{
	if [[ $BENCHMARK == "LRB" || $BENCHMARK == "LRBVB" || $BENCHMARK == "NYT" || $BENCHMARK == "NYTDQ" || $BENCHMARK == "NYTODQ" ]]; then
		RUN_COMMAND="$RUN_COMMAND --data_path="${PATH_TO_DATA}""
	fi
	if [[ $LATENCY_TYPE == "Processing" ]]; then
		RUN_COMMAND="$RUN_COMMAND --use_processing_latency"
	fi

	EXP_DATE_TIME=$(date +"%Y_%m_%d_%H%M")	
	if [[ $PERF_ENABLED == "true" ]]; then
		RUN_COMMAND="perf stat -I 100 --cpu $WORKER_CPU_LIST -d -x ; -o metrics/perf_stat_${SCHEDULING_MODE}_${PROCESSING_MODE}_${PRIORITY_TYPE}_${NUM_QUERIES}_${EXP_DATE_TIME}.txt $RUN_COMMAND"
	fi
	if [[ $USE_TIMEOUT == "true" ]]; then
		PER_QUERY_TIMEOUT=$((DURATION_SEC + 30))
		TIMEOUT_DURATION=$((PER_QUERY_TIMEOUT * NUM_REPETITIONS))
		RUN_COMMAND="timeout $TIMEOUT_DURATION $RUN_COMMAND"
	fi
	RUN_COMMAND="$RUN_COMMAND --src_reservoir_size $SRC_RESERVOIR_SIZE"
	echo "Running command '$RUN_COMMAND' at $EXP_DATE_TIME"
	$RUN_COMMAND
	EXIT_CODE=$?
	echo "Command ran with exit code: $EXIT_CODE"
	echo "=============================="
	if [ ${EXIT_CODE} -ne 0 ] && [ ${EXIT_CODE} -ne 124 ]; then
		echo "Exiting program with exit code ${EXIT_CODE}"
		exit $EXIT_CODE
	fi
	sleep $SLEEP_DELAY
	echo ""
	echo ""
}

echo "Running benchmark $BENCHMARK $NUM_REPETITIONS times using the program enjima_benchmarks in $TARGET_DIR"
echo "Selected scheduling modes : $SCHEDULING_MODES"

if [[ $EXP_TYPE == "config_specific" ]]; then
	echo "Selected processing modes : [TB: $TB_PROCESSING_MODES, SB: $SB_PROCESSING_MODES]"
	echo "General parameters: [Backpressure Threshold: (TB: $TB_BACKPRESSURE_THRESHOLD, SB: $SB_BACKPRESSURE_THRESHOLD), Input Rates: $INPUT_RATES, Max Memory: $MAX_MEMORY]"
	echo "Use query specific number of workers : $QUERY_SPECIFIC_NUM_WORKERS"
else
	echo "Selected processing modes : $PROCESSING_MODES"
	echo "General parameters: [Backpressure Threshold: $BACKPRESSURE_THRESHOLD, Input Rates: $INPUT_RATES, Max Memory: $MAX_MEMORY]"
fi
echo "Main SB parameters: [Scheduling Period: $SCHEDULING_PERIOD ms, Preempt Modes: $PREEMPT_MODES, Priority Types: $PRIORITY_TYPES]"
echo "Other SB parameters: [Max Idle Threshold: $MAX_IDLE_THRESHOLD_MS ms]"
echo "Additional parameters: [Num Workers: $NUM_WORKERS, Num Queries: $NUM_QUERIES_ARR, Latency Reporting Resolution: $LATENCY_RES]"
echo "Press [Ctrl + C] now or forever hold your peace! ..."
sleep 5
echo ""

if [[ $DIRECTIVES == *"clean"* ]]; then
	echo "Cleaning 'metrics' and 'logs' directories..."
	CURRENT_DATE_TIME=$(date +"%Y_%m_%d_%H%M")
	(cd metrics; tar czf metrics_${CURRENT_DATE_TIME}.tar.gz *.csv *.txt; rm *.csv *.txt)
	echo "Archived metrics to file metrics/metrics_${CURRENT_DATE_TIME}.tar.gz"
	(cp conf/*.yaml *.yaml logs; cd logs; tar czf logs_${CURRENT_DATE_TIME}.tar.gz *.log* *.yaml; rm *.log* *.yaml)
	echo "Archived logs and configurations to file logs/logs_${CURRENT_DATE_TIME}.tar.gz"
	echo ""
fi

PERF_ENABLED="false"
if [[ $DIRECTIVES == *"perf"* ]]; then
	PERF_ENABLED="true"
fi

START_DATE_TIME=$(date +"%Y_%m_%d_%H%M")
SLEEP_DELAY=10

if [[ $EXP_TYPE == "config_specific" ]]; then
	RUN_COMMAND_INVARIANT="$TARGET_DIR/enjima_benchmarks -b ${BENCHMARK} -t $DURATION_SEC -m $MAX_MEMORY -r $NUM_REPETITIONS --latency_res $LATENCY_RES --events_per_block $EVENTS_PER_BLOCK --blocks_per_chunk $BLOCKS_PER_CHUNK"
	for INPUT_RATE in $INPUT_RATES; do
		for LATENCY_EMIT_PERIOD_MS in $LATENCY_VALUES; do
			for SCHEDULING_MODE in $SCHEDULING_MODES; do	
				RUN_COMMAND_BASE="$RUN_COMMAND_INVARIANT -i $INPUT_RATE -l $LATENCY_EMIT_PERIOD_MS -s $SCHEDULING_MODE"
				if [[ $SCHEDULING_MODE == "TB" ]]; then
					for PROCESSING_MODE in $TB_PROCESSING_MODES; do
						for NUM_QUERIES in $NUM_QUERIES_ARR; do
							sleep $SLEEP_DELAY
							if [[ $QUERY_SPECIFIC_NUM_WORKERS == "true" ]]; then
								EVENT_GEN_CPU_LIST=$(yq e .benchmarkConfig.${BENCHMARK}.specific.q${NUM_QUERIES}.eventGenCpuList < $SETUP_FILE)
								sed -i "s/eventGenCpuList:.*/eventGenCpuList: \"${EVENT_GEN_CPU_LIST}\"/g" $CONF_FILE
								WORKER_CPU_LIST=$(yq e .benchmarkConfig.${BENCHMARK}.specific.q${NUM_QUERIES}.workerCpuList < $SETUP_FILE)
								sed -i "s/workerCpuList:.*/workerCpuList: \"${WORKER_CPU_LIST}\"/g" $CONF_FILE
								NUM_WORKERS=$(yq e .benchmarkConfig.${BENCHMARK}.specific.q${NUM_QUERIES}.numWorkers < $SETUP_FILE)
							fi
							RUN_COMMAND="$RUN_COMMAND_BASE -p $PROCESSING_MODE --backpressure_threshold $TB_BACKPRESSURE_THRESHOLD -q $NUM_QUERIES -w $NUM_WORKERS"
							execute_run_command
						done
					done
				else		
					for PROCESSING_MODE in $SB_PROCESSING_MODES; do
						for PREEMPT_MODE in $PREEMPT_MODES; do
							for PRIORITY_TYPE in $PRIORITY_TYPES; do
								for NUM_QUERIES in $NUM_QUERIES_ARR; do
									sleep $SLEEP_DELAY
									if [[ $QUERY_SPECIFIC_NUM_WORKERS == "true" ]]; then
										EVENT_GEN_CPU_LIST=$(yq e .benchmarkConfig.${BENCHMARK}.specific.q${NUM_QUERIES}.eventGenCpuList < $SETUP_FILE)
										sed -i "s/eventGenCpuList:.*/eventGenCpuList: \"${EVENT_GEN_CPU_LIST}\"/g" $CONF_FILE
										WORKER_CPU_LIST=$(yq e .benchmarkConfig.${BENCHMARK}.specific.q${NUM_QUERIES}.workerCpuList < $SETUP_FILE)
										sed -i "s/workerCpuList:.*/workerCpuList: \"${WORKER_CPU_LIST}\"/g" $CONF_FILE
										NUM_WORKERS=$(yq e .benchmarkConfig.${BENCHMARK}.specific.q${NUM_QUERIES}.numWorkers < $SETUP_FILE)
									fi
									RUN_COMMAND="$RUN_COMMAND_BASE --preempt_mode $PREEMPT_MODE --priority_type $PRIORITY_TYPE \
--scheduling_period $SCHEDULING_PERIOD --max_idle_threshold_ms $MAX_IDLE_THRESHOLD_MS -p $PROCESSING_MODE --backpressure_threshold $SB_BACKPRESSURE_THRESHOLD -q $NUM_QUERIES -w $NUM_WORKERS"
									execute_run_command
								done
							done
						done
					done
				fi
			done
			echo "Finished benchmark for latency emit period of $LATENCY_EMIT_PERIOD_MS ms"
			echo ""
		done
	done
elif [[ $EXP_TYPE == "sensitivity_analysis" ]]; then
	RUN_COMMAND_INVARIANT="$TARGET_DIR/enjima_benchmarks -b ${BENCHMARK} -t $DURATION_SEC -m $MAX_MEMORY -r $NUM_REPETITIONS --latency_res $LATENCY_RES -w $NUM_WORKERS"
	LATENCY_EMIT_PERIOD_MS=$LATENCY_VALUES
	for INPUT_RATE in $INPUT_RATES; do
		for EBP in $EVENTS_PER_BLOCK; do
			for BPC in $BLOCKS_PER_CHUNK; do
				for SCHEDULING_MODE in $SCHEDULING_MODES; do	
					RUN_COMMAND_BASE="$RUN_COMMAND_INVARIANT -i $INPUT_RATE -l $LATENCY_EMIT_PERIOD_MS -s $SCHEDULING_MODE --backpressure_threshold $BACKPRESSURE_THRESHOLD"
					if [[ $SCHEDULING_MODE == "TB" ]]; then
						for PROCESSING_MODE in $PROCESSING_MODES; do
							for NUM_QUERIES in $NUM_QUERIES_ARR; do
								sleep $SLEEP_DELAY
								RUN_COMMAND="$RUN_COMMAND_BASE -p $PROCESSING_MODE -q $NUM_QUERIES --events_per_block $EBP --blocks_per_chunk $BPC"
								execute_run_command
							done
						done
					else		
						for PROCESSING_MODE in $PROCESSING_MODES; do
							for PREEMPT_MODE in $PREEMPT_MODES; do
								for PRIORITY_TYPE in $PRIORITY_TYPES; do
									for SP in $SCHEDULING_PERIOD; do
										for NUM_QUERIES in $NUM_QUERIES_ARR; do
											sleep $SLEEP_DELAY
											RUN_COMMAND="$RUN_COMMAND_BASE --preempt_mode $PREEMPT_MODE --priority_type $PRIORITY_TYPE \
	--scheduling_period $SP --max_idle_threshold_ms $MAX_IDLE_THRESHOLD_MS -p $PROCESSING_MODE -q $NUM_QUERIES --events_per_block $EBP --blocks_per_chunk $BPC"
											execute_run_command
										done
									done
								done
							done
						done
					fi
				done
				echo "Finished benchmark for latency emit period of $LATENCY_EMIT_PERIOD_MS ms"
				echo ""
			done
		done
	done
else
	RUN_COMMAND_INVARIANT="$TARGET_DIR/enjima_benchmarks -b ${BENCHMARK} -t $DURATION_SEC -m $MAX_MEMORY -r $NUM_REPETITIONS \
--backpressure_threshold $BACKPRESSURE_THRESHOLD --latency_res $LATENCY_RES -w $NUM_WORKERS --events_per_block $EVENTS_PER_BLOCK --blocks_per_chunk $BLOCKS_PER_CHUNK"
	for INPUT_RATE in $INPUT_RATES; do
		for LATENCY_EMIT_PERIOD_MS in $LATENCY_VALUES; do
			for PROCESSING_MODE in $PROCESSING_MODES; do
				for SCHEDULING_MODE in $SCHEDULING_MODES; do
					for NUM_QUERIES in $NUM_QUERIES_ARR; do
						RUN_COMMAND_BASE="$RUN_COMMAND_INVARIANT -i $INPUT_RATE  -l $LATENCY_EMIT_PERIOD_MS \
-p $PROCESSING_MODE -s $SCHEDULING_MODE -q $NUM_QUERIES"
						if [[ $SCHEDULING_MODE == "TB" ]]; then
							sleep $SLEEP_DELAY
							RUN_COMMAND=$RUN_COMMAND_BASE
							execute_run_command
						else	
							for PREEMPT_MODE in $PREEMPT_MODES; do
								for PRIORITY_TYPE in $PRIORITY_TYPES; do
									sleep $SLEEP_DELAY
									RUN_COMMAND="$RUN_COMMAND_BASE \
--preempt_mode $PREEMPT_MODE --priority_type $PRIORITY_TYPE --scheduling_period $SCHEDULING_PERIOD --max_idle_threshold_ms $MAX_IDLE_THRESHOLD_MS"
									execute_run_command
								done
							done
						fi
					done
				done
			done
			echo "Finished benchmark for latency emit period of $LATENCY_EMIT_PERIOD_MS ms"
			echo ""
		done
	done
fi

if [[ $DIRECTIVES == *"metrics"* ]]; then
	archive_metrics
fi
echo "Done."
