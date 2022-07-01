# Redit-Benchmark

*Benchmarks of distributed systems*

## Components

| benchmark | create_time | reference | role |
| :----: | :----: | :----: | :----: |
|         MapReduce         | 2022_03_30 (deprecated) |          https://github.com/OneSizeFitsQuorum/MIT6.824-2021 | coordinator、worker |
|         Raft-Java         |       2022_04_27        |          https://github.com/wenweihu86/raft-java    | client、server |
|      Distributed-Id       | 2022_04_30 (deprecated) |          https://github.com/beyondfengyu/DistributedID   | client、server |
|        JLiteSpider        | 2022_05_14 (deprecated) |    https://github.com/luohaha/jlitespider     |  spider、lighter、rabbitmq  |
|      Zookeeper-3.7.1      |       2022_05_24        |          https://github.com/apache/zookeeper        |        master、slave        |
|        Kafka-3.2.0        |       2022_05_28        |          https://github.com/apache/kafka            |        master、slave        |
|       Hbase-2.4.12        |       2022_06_01        |          https://github.com/apache/hbase            |    master、regionserver     |
|       hadoop-mapreduce    |       2022_06_08        |          https://github.com/apache/hadoop           |    namenode、datanode    |
|       elasticsearch-8.22  |       2022_06_18        |          https://github.com/elastic/elasticsearch   |    client、server   |
|       hazelcast-5.1.2     |       2022_06_20        |          https://github.com/hazelcast/hazelcast     |    client、server   |
|       cassandra-3.11.6    |       2022_06_27        |          https://github.com/apache/cassandra        |    master、slave    |
|       rocketmq-4.9.4      |       2022_06_30        |          https://github.com/apache/rocketmq         |    master、slave    |
|       activemq-5.16.5     |       2022_07_01        |          https://github.com/apache/activemq         |    master、slave    |


## MapReduce

A `Coordinator`, and one or more `Worker` processes executing in parallel. The `Worker` will interact with the `Coordinator` via RPC.The `Coordinator` is responsible for assigning tasks and noting that a `Worker` completes its tasks in a reasonable amount of time, and recycles if not.Each `worker` process requests a task from the `Coordinator`, reads the task's input from one or more mapreduce.files, executes the task, and writes the task's output to one or more mapreduce.files.

#### Role

**Coordinator :**

- Create a temporary file directory and an output directory.
  
- Start RPC service with thread pool to provide socket connection service for worker.
  
- Maintain multiple Task-related queues and collections and monitor their status.
  
- Assign map and reduce tasks to Workers, and monitor the recycling of tasks that execute timeouts.


**Worker :**

- The loop asks the Coordinator for the Task, and after the execution is completed, it is verified whether it is completed.


- It is divided into two operation modes: map and reduce. When the map mode is completed, the intermediate results are written to the temp file, and then switched to the reduce mode. When the reduce mode is completed, the final result file is output.


#### Difficulty

1. Implementation of data structures: custom KeyValue, doubly circular linked list, blockQueue, mapSet, etc

2. Implementation of RPC communication between each Worker and Coordinator

3. Concurrency implementation: Lock, ReentrantLock, Condition, etc

4. Unknown bug



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


#### Difficulty

1. Implement Raft leader election and heartbeat

2. Implement the leader and follower code to append new log entries

3. Raft keep persistent state that survives a reboot

4. Store a "snapshot" of their state from time to time



## Distributed-ID

Provides a lightweight, high-concurrency, and high-availability service for generating unique IDs. The generated ID is a 64-bit long integer that is globally unique, keeps increasing, and is relatively ordered.

#### Features

1. Generate ID based on twitter's snowflake algorithm

2. Provide communication layer access based on netty framework

3. Provide HTTP and SDK access in two ways

4. Lightweight, high concurrency, easy to scale

5. Simple deployment and support for distributed deployment


## JLiteSpider

A lite distributed Java spider framework.

- JLiteSpider is inherently distributed, and each worker needs to be connected through one or more message queues. My choice for message queue is rabbitmq. There can be one-to-one, one-to-many, many-to-one or many-to-many relationships between workers and messages, all of which can be configured freely and simply. There are four types of messages stored in the message queue: url, page source code, parsed results, and custom messages. Similarly, the work of workers is also divided into four parts: downloading pages, parsing pages, data persistence and custom operations.

- Users only need to specify the relationship between workers and message queues in the configuration file. Then in the code, define the four parts of the worker's work. You can complete the writing of the crawler.

#### Usage

1. Start rabbitmq.

2. Define the relationship between workers and message queues in the configuration file.

3. Write the worker's job in code.

4. Finally, start the spiders and lighters.
