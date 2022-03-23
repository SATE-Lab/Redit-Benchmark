# Redit-Benchmark

*Benchmarks of distributed systems*

## MapReduce

- A `Coordinator`, and one or more `Worker` processes executing in parallel. The `Worker` will interact with the `Coordinator` via RPC.


- The `Coordinator` is responsible for assigning tasks and noting that a `Worker` completes its tasks in a reasonable amount of time, and recycles if not.


- Each `worker` process requests a task from the `Coordinator`, reads the task's input from one or more files, executes the task, and writes the task's output to one or more files.



**Difficulty:**

1. Implementation of data structures: custom KeyValue, doubly circular linked list, blockQueue, mapSet, etc

2. RPC implementation (requires learning)

3. Concurrency implementation: Lock, ReentrantLock, Condition, etc

4. Unknown bug