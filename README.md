# Recommender

## 项目简介

[docker registry][1]支持把push, pull的events发送给webhook，这个被称作[registry notification][2], event数据包含用户及操作相关的信息，比如说event发生的时间，用户，action(pull/push)，所操作的docker repository的路径，manifest等等。这些信息给用户行为分析提供了丰富的素材。此项目的目的就是分析历史数据，对用户即将pull的docker image进行预测，从而给用户推荐他可能会感兴趣的image。

## Elasticsearch与Spark的整合

[spark][1]提供快速和大型数据处理的引擎，elasticsearch是性能优功能全的搜索引擎。Spark拥有与elasticsearch交互的插件，以下是本项目涉及到的一些feature:
* [MLlib](https://spark.apache.org/mllib/), a scalable machine learning library,
* [Spark SQL](https://spark.apache.org/sql/), a unified access platform for structured big data,
* [Spark Streaming](https://spark.apache.org/streaming/), a library to build scalable fault-tolerant streaming applications.

### Architecture:

### Machine Learning

### Real-Time Stream Processing and Elasticsearch

## How to run

### Environment Deployment


### Run 
```
mvn package
```

* run from local
```
spark-submit --class com.cloudera.datascience.recommender.RunRecommender --master local --driver-memory 6g <jarfile>.jar <data folder>
```

* run from spark on marathon
```
dcos spark run --verbose --submit-args='--driver-memory 6g --class <jarfile>.jar <elasticsearch node:port>'
```

## Reference
[1]: https://github.com/docker/distribution
[2]: https://docs.docker.com/registry/notifications/
[3]: http://spark.apache.org/
