# Enjima Stream Processing

This repository contains the scripts and source code required to run YSB, LRB, and NYT benchmarks against the <a href="https://github.com/lasanthafdo/enjima-library">Enjima stream processing engine</a>. 

## Preparing Build
You need the same compilation tools and versions as those required to build the `enjima-library` to build this repository. There are some files to prepare before executing benchmarks. 

### Creating <code>lib</code> folder
If the <code>lib</code> folder does not exist, create one. 
Then, copy following files generated after building <code>enjima-library</code>. 
Refer to <a href="https://github.com/lasanthafdo/enjima-library">this link</a> for details on how to build <code>enjima-library</code>
```
libEnjimaDebug.a      (if you built enjima with Debug mode)
libEnjimaRelease.a    (if you build enjima with Release mode)
```

### Creating <code>.yaml</code> files
Copy the content within <code>bench.yaml.sample</code> into <code>bench.yaml</code>:
```
$ cat bench.yaml.sample > bench.yaml
```
Adjust the configuration parameters within bench.yaml as required.

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

| Arguments | Description                                                                         |
| :--------- |:------------------------------------------------------------------------------------|
| <code>BENCHMARK</code> | The type of benchmark to run (e.g. YSB)                                             |
| <code>DURATION_SEC</code> | The duration of bechmark in seconds                                                 |
| <code>NUM_REPETITIONS</code> | The number of times to repeat the benchmark                                         |
| <code>DIRECTIVES</code> | Parameters for specific configuration                                               |
| | <code>perf</code>: enabling Linux `perf` performance monitoring                     |
| | <code>clean</code>: cleaning the <code>logs</code> and <code>metrics</code> folders |
| | <code>metrics:</code> archives the metrics collected                                |

For example
```
./run_benchmark.sh YSB 60 5 clean
```
This command will run YSB Benchmark on <code>enjima</code> for 60 seconds and repeat this process five times.
