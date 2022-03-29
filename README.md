# Redit-Benchmark

*Benchmarks of distributed systems*

## MapReduce

- A `Coordinator`, and one or more `Worker` processes executing in parallel. The `Worker` will interact with the `Coordinator` via RPC.


- The `Coordinator` is responsible for assigning tasks and noting that a `Worker` completes its tasks in a reasonable amount of time, and recycles if not.


- Each `worker` process requests a task from the `Coordinator`, reads the task's input from one or more mapreduce.files, executes the task, and writes the task's output to one or more mapreduce.files.


### Role

**Coordinator :**

- Create a temporary file directory and an output directory.
  

- Start RPC service with thread pool to provide socket connection service for worker.
  

- Maintain multiple Task-related queues and collections and monitor their status.
  

- Assign map and reduce tasks to Workers, and monitor the recycling of tasks that execute timeouts.


**Worker :**

- The loop asks the Coordinator for the Task, and after the execution is completed, it is verified whether it is completed.


- It is divided into two operation modes: map and reduce. When the map mode is completed, the intermediate results are written to the temp file, and then switched to the reduce mode. When the reduce mode is completed, the final result file is output.


### Difficulty

1. Implementation of data structures: custom KeyValue, doubly circular linked list, blockQueue, mapSet, etc

2. Implementation of RPC communication between each Worker and Coordinator

3. Concurrency implementation: Lock, ReentrantLock, Condition, etc

4. Unknown bug

