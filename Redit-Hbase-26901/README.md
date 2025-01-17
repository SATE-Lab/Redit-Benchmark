# Redit-HBASE-26901

### Details

Title: delete with null columnQualifier occurs NullPointerException when NewVersionBehavior is on

|         Label         |        Value        |      Label      |         Value          |
|:---------------------:|:-------------------:|:---------------:|:----------------------:|
|       **Type**        |         Bug         |  **Priority**   |         Major          |
|      **Status**       |      RESOLVED       | **Resolution**  |         Fixed          |
| **Affects Version/s** | 3.0.0-alpha-2, 2.4.11 | **Component/s** |     Deletes, Scanners     |

### Description

since  HBASE-15616, setting column qualifier as null is possible, but when NewVersionBehavior is on, delete with null columnQualifier occurs NullPointerException.

```
@Test
public void testNullColumnQualifier() throws IOException {
  try (Table t = createTable()) {
    Delete del = new Delete(ROW);
    del.addColumn(FAMILY, null);
    t.delete(del);
    Result r = t.get(new Get(ROW)); //NPE happens.
    assertTrue(r.isEmpty());
  }
} 
```

### Testcase

Start an hbase cluster, connect to the cluster and get the admin object. Setting NewVersionBehavior to true when creating a table and deleting with null columnQualifier will result in a NullPointerException as follows:

```
17:25:56.918 [main] DEBUG org.apache.hadoop.hbase.client.RpcRetryingCallerImpl - Call exception, tries=6, retries=16, started=4279 ms ago, cancelled=false, msg=java.io.IOException
	at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:460)
	at org.apache.hadoop.hbase.ipc.CallRunner.run(CallRunner.java:133)
	at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:359)
	at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:339)
Caused by: java.lang.NullPointerException
	at org.apache.hadoop.hbase.regionserver.querymatcher.NewVersionBehaviorTracker.add(NewVersionBehaviorTracker.java:214)
	at org.apache.hadoop.hbase.regionserver.querymatcher.NormalUserScanQueryMatcher.match(NormalUserScanQueryMatcher.java:73)
	at org.apache.hadoop.hbase.regionserver.StoreScanner.next(StoreScanner.java:625)
	at org.apache.hadoop.hbase.regionserver.KeyValueHeap.next(KeyValueHeap.java:155)
	at org.apache.hadoop.hbase.regionserver.HRegion$RegionScannerImpl.populateResult(HRegion.java:7400)
	at org.apache.hadoop.hbase.regionserver.HRegion$RegionScannerImpl.nextInternal(HRegion.java:7568)
	at org.apache.hadoop.hbase.regionserver.HRegion$RegionScannerImpl.nextRaw(HRegion.java:7332)
	at org.apache.hadoop.hbase.regionserver.HRegion$RegionScannerImpl.next(HRegion.java:7309)
	at org.apache.hadoop.hbase.regionserver.HRegion$RegionScannerImpl.next(HRegion.java:7296)
	at org.apache.hadoop.hbase.regionserver.RSRpcServices.get(RSRpcServices.java:2659)
	at org.apache.hadoop.hbase.regionserver.RSRpcServices.get(RSRpcServices.java:2584)
	at org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos$ClientService$2.callBlockingMethod(ClientProtos.java:45815)
	at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:392)
	... 3 more
```