# Redit-HBASE-21487

### Details

Title: Concurrent modify table ops can lead to unexpected results

|         Label         |        Value        |      Label      |         Value          |
|:---------------------:|:-------------------:|:---------------:|:----------------------:|
|       **Type**        |         Bug         |  **Priority**   |         Major          |
|      **Status**       |      RESOLVED       | **Resolution**  |         Fixed          |
| **Affects Version/s** | 3.0.0-alpha-1, 2.0.0  | **Component/s** |        None         |

### Description

Concurrent modifyTable or add/delete/modify columnFamily leads to incorrect result. After HBASE-18893, The behavior of add/delete/modify column family during concurrent operation is changed compare to branch-1.When one client is adding cf2 and another one cf3 .. In branch-1 final result will be cf1,cf2,cf3 but now either cf1,cf2 OR cf1,cf3 will be the outcome depending on which ModifyTableProcedure executed finally.Its because new table descriptor is constructed before submitting the ModifyTableProcedure in HMaster class and its not guarded by any lock.

**Steps to reproduce**
1. Create table 't' with column family 'f1'
2. Client-1 and Client-2 requests to add column family 'f2' and 'f3' on table 't' concurrently.

**Expected Result**
Table should have three column families(f1,f2,f3)

**Actual Result**
Table 't' will have column family either (f1,f2) or (f1,f3)

### Testcase

Start an hbase cluster, get the cluster connection to get the admin object, use column family "A" to create table "t1", Client-1 and Client-2 request to add column families "B" and "C" to table "t1" at the same time, The expected table should have three column families (A, B, C), the actual result is two column families (A, C).