# Redit-Kafka-13996

### Details

Title: log.cleaner.io.max.bytes.per.second cannot be changed dynamically

|         Label         |   Value   |      Label       |           Value           |
|:---------------------:|:---------:|:----------------:|:-------------------------:|
|       **Type**        |    Bug    |   **Priority**   |           Major           |
|      **Status**       | RESOLVED  |  **Resolution**  |           Fixed           |
| **Affects Version/s** |   3.2.0   | **Component/s**  | config, core, log cleaner |

### Description

- log.cleaner.io.max.bytes.per.second cannot be changed dynamically using bin/kafka-configs.sh
- Reproduction procedure:\
  1. Create a topic with cleanup.policy=compact 
        ```
         bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic my-topic --config cleanup.policy=compact --config cleanup.policy=compact --config segment.bytes=104857600 --config compression.type=producer
        ```
  2. Change log.cleaner.io.max.bytes.per.second=10485760 using bin/kafka-configs.sh
        ```
         bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --alter --add-config log.cleaner.io.max.bytes.per.second=10485760
        ```
  3. Send enough messages(> segment.bytes=104857600) to activate Log Cleaner
  4. logs/log-cleaner.log, configuration by log.cleaner.io.max.bytes.per.second=10485760 is not reflected and Log Cleaner does not slow down (>= log.cleaner.io.max.bytes.per.second=10485760).
        ```
     [2022-06-15 14:52:14,988] INFO [kafka-log-cleaner-thread-0]:
          Log cleaner thread 0 cleaned log my-topic-0 (dirty section = [39786, 81666])
          3,999.0 MB of log processed in 2.7 seconds (1,494.4 MB/sec).
          Indexed 3,998.9 MB in 0.9 seconds (4,218.2 Mb/sec, 35.4% of total time)
          Buffer utilization: 0.0%
          Cleaned 3,999.0 MB in 1.7 seconds (2,314.2 Mb/sec, 64.6% of total time)
          Start size: 3,999.0 MB (41,881 messages)
          End size: 0.1 MB (1 messages)
          100.0% size reduction (100.0% fewer messages)
     (kafka.log.LogCleaner)
        ```
- Problem cause:
  - log.cleaner.io.max.bytes.per.second is used in Throttler in LogCleaner, however, it is only passed to Throttler at initialization time.
    - https://github.com/apache/kafka/blob/4380eae7ceb840dd93fee8ec90cd89a72bad7a3f/core/src/main/scala/kafka/log/LogCleaner.scala#L107-L112
  - Need to change Throttler configuration value at reconfigure() of LogCleaner.
    - https://github.com/apache/kafka/blob/4380eae7ceb840dd93fee8ec90cd89a72bad7a3f/core/src/main/scala/kafka/log/LogCleaner.scala#L192-L196
- A workaround is that restarting every broker adding log.cleaner.io.max.bytes.per.second to config/server.properties

### Testcase

Start zookeeper in a three-node cluster using KRaft, then create a topic 'test'. 
After that, we change log.cleaner.io.max.bytes.per.second to a relatively small value.
Then we create consumer and producer, and send enough message to trigger log cleaner.
The result shows that the speed of log cleaner exceeds the value we set, meaning that this value cannot be changed dynamically.