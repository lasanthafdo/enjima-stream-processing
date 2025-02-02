# enjima-benchmarks
<!--
A benchmark suite for Enjima
```
Usage:
  enjima_benchmarks [OPTION...]

 default options:
  -d, --debug                 Enable debugging
  -b, --bench arg             Benchmark name (default: lrb)
  -t, --duration arg          Benchmark duration (in seconds) (default: 30)
  -r, --repeat arg            Number of times to repeat (default: 1)
  -m, --memory arg            Amount of memory for data exchange (in 
                              megabytes) (default: 64)
      --events_per_block arg  Number of events per memory block (default: 
                              1000)
      --blocks_per_chunk arg  Number of blocks per memory chunk (default: 
                              16)
      --batch                 Use batch mode (default: true)
  -h, --help                  Print usage
```

For example, if you want to run the YSB benchmark for 30 seconds with 512 MiB of memory, with other parameters as the default, run the following command against the built binary in a shell.

```
./enjima_benchmarks -b ysb -t 30 -m 512
```
-->

## Preparing Build
There are some files to prepare before executing benchmarks.

### Creating <code>lib</code> folder
If the <code>lib</code> folder does not exist, create one. Then, copy following giles generated after building <code>enjima</code>. Refer to <a href="https://github.com/lasanthafdo/enjima">this link</a> for details on how to build <code>enjima</code>
```
libEnjimaDebug.a      (if you built enjima with Debug mode)
libEnjimaRelease.a    (if you build enjima with Release mode)
```

### Creating <code>.yaml</code> files
Copy the content within <code>bench.yaml.sample</code> into <code>bench.yaml</code>:
```
$ cat bench.yaml.sample > bench.yaml
```
Do the same for <code>enjima-config.yaml</code> within <code>conf</code> directory:
```
$ cat ./conf/enjima-config.yaml.sample > ./conf/enjima-config.yaml
```

### Creating other folders
Create <code>logs</code> and <code>metrics</code> folders:
```
$ mkdir logs && mkdir metrics
```


## Build using CMake
First, create a directory for the build files. Ensure the directory name matches the <code>targetDir</code> specified in <code>bench.yaml</code>:
```
$ mkdir cmake-build-release && cmake-build-release
```

Then,configure the build by specifying the build type (either <code>Release</code> or <code>Build</code>):
```
$ cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) ..
```

Finally, compile <code>enjima-benchmark</code> executable:
```
$ cmake --build . -j 12
```

## Executing Benchmarks
Use the following command to execute benchmakrs:
```
./run_benchmark.sh BENCHMARK DURATION_SEC NUM_REPETITIONS DIRECTIVES
```

| Arguments | Description |
| :--------- | :----------- |
| <code>BENCHMARK</code> | The type of benchmark to run (e.g. YSB) |
| <code>DURATION_SEC</code> | The duration of bechmark in seconds |
| <code>NUM_REPETITIONS</code> | The number of times to repeat the benchmark |
| <code>DIRECTIVES</code> | Parameters for specific configuration |
| | <code>perf</code>: enabling performance monitoring |
| | <code>clean</code>: cleaning the <code>logs</code> and <code>metrics</code> folders|
| | <code>metrics:</code> archives the metrics collected |

For example
```
./run_benchmark.sh YSB 30 2 perf
```
This command will run YSB Benchmark on <code>enjima</code> for 30 seconds and repeat this process twice.