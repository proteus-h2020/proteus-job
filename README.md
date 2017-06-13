# The Proteus Job

This is the Flink Job that powers Proteus H2020 analytics stack.

### Dependencies
1. Install Proteus Engine (forked version of Apache Flink 1.4-SNAPSHOT)
```shell
git clone https://github.com/proteus-h2020/proteus-engine -b proteus-dev
cd proteus-engine
mvn clean install -DskipTests # you may need to disable checkstyle mvn plugin
cd flink-dist
mvn clean install
```
2. Install Solma

```shell
git clone https://github.com/proteus-h2020/SOLMA.git -b develop
cd SOLMA
mvn clean install
```

### How to build

```shell
mvn clean package -DskipTests
```

This will produce a shaded jar called ```proteus-job_2.10-0.1-SNAPSHOT.jar``` in ```target/``` dir. This jar can be submitted to a proteus-engine cluster.

### How to run it on a cluster

```
./flink run <JOB_PARAMS> proteus-job_2.10-0.1-SNAPSHOT.jar --bootstrap-server <KAFKA_BOOTSTRAP_SERVER>
```

You find a complete list of JOB_PARAMS in [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/cli.html).

#### Optional job-specific parameters

1. **state-backend**	Flink State Backend [memory|rocksdb]
2. **state-backend-mbsize**	Flink Memory State Backend size in MB (default: 20)
3. **flink-checkpoints-dir** A valid directory in which Flink will store Rocksdb checkpoints, e.g., hdfs://namenode:9000/flink-checkpoints/
4. **flink-checkpoints-interval**	Flink Checkpoints Interval in mins (default: 10)
### How to run it within your IDE

In order to local test your job within your IDe, you need to select ```ide-testing``` maven profile.
