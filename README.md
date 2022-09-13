# Redit-Benchmark

*Benchmarks of distributed systems*

## Components

| benchmark | create_time | reference |
| :----: | :----: | :----: | :----: | :----: |
|         Raft-Java         |       2022_04_27        |          https://github.com/wenweihu86/raft-java    |
|      Zookeeper-3.7.1      |       2022_05_24        |          https://github.com/apache/zookeeper        |
|        Kafka-3.2.0        |       2022_05_28        |          https://github.com/apache/kafka            |
|       Hbase-2.4.12        |       2022_06_01        |          https://github.com/apache/hbase            |
|       hadoop-hdfs         |       2022_06_08        |          https://github.com/apache/hadoop           |
|       elasticsearch-8.22  |       2022_06_18        |          https://github.com/elastic/elasticsearch   |
|       hazelcast-5.1.2     |       2022_06_20        |          https://github.com/hazelcast/hazelcast     |
|       cassandra-3.11.6    |       2022_06_27        |          https://github.com/apache/cassandra        |
|       rocketmq-4.9.4      |       2022_06_30        |          https://github.com/apache/rocketmq         |
|       activemq-5.16.5     |       2022_07_01        |          https://github.com/apache/activemq         |


## Raft-Java

Building a fault-tolerant key/value database with Raft.

#### Features

1. leader election

2. log replication

3. snapshot

4. Cluster membership changes dynamically 


#### Role

**Server :**

- Use multiple servers to form a raft cluster, and elect the leader by voting according to the raft protocol.
  
- Realize data writing and reading, data is written to the raft cluster synchronously, and the main logic is implemented by the specific application state machine.
  
- Snapshot the data in the state machine, and call each node locally at regular intervals.


**Client :**

- Test the storage service of the raft cluster to realize the writing and reading of key-value pairs.



## Zookeeper-3.7.1

Apache ZooKeeper is an effort to develop and maintain an open-source server which enables highly reliable distributed coordination.

#### Role

**Leader :**

- The only scheduler and processor of transaction requests (write operations), ensuring the order of cluster transaction processing.
  
- The scheduler of each server within the cluster.
  
- For the write operation request, it needs to be forwarded to the leader for processing. The leader needs to decide the number and execute the operation.


**Follower :**

- Process client non-transaction (read operation) requests, forward transaction requests to Leader

- Participate in cluster Leader election voting 2n-1 units can vote for the cluster.


**Observer :**

- For zookeeper clusters with a large number of visits, the observer role can also be added.

- Observe the latest state changes of the Zookeeper cluster and synchronize these states, which can be processed independently for non-transactional requests, and forwarded to the Leader server for processing for transactional requests.

- It will not participate in any form of voting and only provide non-transactional services, which are usually used to improve the non-transactional processing capability of the cluster without affecting the transactional processing capability of the cluster.



## Kafka-3.2.0

Apache Kafka is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

#### Role

**Server :**

- Kafka is run as a cluster of one or more servers that can span multiple datacenters or cloud regions. Some of these servers form the storage layer, called the brokers.
  
- Other servers run Kafka Connect to continuously import and export data as a stream of events to integrate Kafka with your existing systems such as relational databases and other Kafka clusters.
  
- Kafka clusters are highly scalable and fault-tolerant.


**Client :**

- They allow you to write distributed applications and microservices that read, write, and process streams of events in a parallel, large-scale, and fault-tolerant manner, even in the event of network problems or machine failures.

- Kafka ships with a few such clients that are enhanced by dozens of clients provided by the Kafka community.



## Hbase-2.4.12

Apache HBase is an open-source, distributed, versioned, column-oriented store modeled after Google' Bigtable.

#### Features

- Linear and modular scalability.

- Strictly consistent reads and writes.

- Automatic and configurable sharding of tables

- Automatic failover support between RegionServers.

- Convenient base classes for backing Hadoop MapReduce jobs with Apache HBase tables.

- Easy to use Java API for client access.

- Block cache and Bloom Filters for real-time queries.

- Query predicate push down via server side Filters

- Thrift gateway and a REST-ful Web service that supports XML, Protobuf, and binary data encoding options

- Extensible jruby-based (JIRB) shell

- Support for exporting metrics via the Hadoop metrics subsystem to files or Ganglia; or via JMX



## hadoop-mapreduce

MapReduce is one of the core components of hadoop. Hadoop needs to be distributed in two parts, one is the distributed file system HDFS, and the other is the distributed computing framework MapReduce. The core function of MapReduce is to integrate the business logic code written by the user and its own default components into a complete distributed computing program, which runs concurrently on a Hadoop cluster.

#### process

- Map is responsible for decomposing a task into multiple tasks for processing. Task allocation will increase the complexity of the program, but the more people and the greater the power, the efficiency will be significantly improved.

- Reduce is responsible for summarizing the results of the map stage.



## elasticsearch-8.22

Elasticsearch is the distributed search and analytics engine at the heart of the Elastic Stack.

#### Features

- Elasticsearch provides near real-time search and analytics for all types of data. Whether you have structured or unstructured text, numerical data, or geospatial data, Elasticsearch can efficiently store and index it in a way that supports fast searches.

- You can go far beyond simple data retrieval and aggregate information to discover trends and patterns in your data. And as your data and query volume grows, the distributed nature of Elasticsearch enables your deployment to grow seamlessly right along with it.

- While not every problem is a search problem, Elasticsearch offers speed and flexibility to handle data in a wide variety of use cases.



## hazelcast-5.1.2

Hazelcast is a distributed computation and storage platform for consistently low-latency querying, aggregation and stateful computation against event streams and traditional data sources.

#### Features

- Stateful and fault-tolerant data processing and querying over data streams and data at rest using SQL or dataflow API.

- A comprehensive library of connectors such as Kafka, Hadoop, S3, RDBMS, JMS and many more.

- Distributed messaging using pub-sub and queues.

- Distributed, partitioned, queryable key-value store with event listeners, which can also be used to store contextual data for enriching event streams with low latency.

- A production-ready Raft-implementation which allows lineralizable (CP) concurrency primitives such as distributed locks.



## cassandra-3.11.6

Apache Cassandra is a highly-scalable partitioned row store. Rows are organized into tables with a required primary key.

#### Features

- Distribution provides power and resilience.

- Want more power? Add more nodes.

- partitions.

- Replication ensures reliability and fault tolerance.

- Tuning your consistency.



## rocketmq-4.9.4

Apache RocketMQ is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability.

#### Features

- Messaging patterns including publish/subscribe, request/reply and streaming.

- Financial grade transactional message

- Built-in fault tolerance and high availability configuration options base on DLedger.

- Versatile big-data and streaming ecosystem integration



## activemq-5.16.5

Apache ActiveMQ is a high performance Apache 2.0 licensed Message Broker and JMS 1.1 implementation.

#### Features

- Supports a variety of Cross Language Clients and Protocols from Java, C, C++, C#, Ruby, Perl, Python, PHP.

- Full support for the Enterprise Integration Patterns both in the JMS client and the Message Broker.

- Supports many advanced features such as Message Groups, Virtual Destinations, Wildcards and Composite Destinations.

- Fully supports JMS 1.1 and J2EE 1.4 with support for transient, persistent, transactional and XA messaging.