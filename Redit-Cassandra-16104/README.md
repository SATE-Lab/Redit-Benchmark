# Redit-Cassandra-16104

### Details

Title: Wrong warning about data volumes capacity

|         Label         |                  Value                   |      Label      |     Value      |
|:---------------------:|:----------------------------------------:|:---------------:|:--------------:|
|       **Type**        |                   Bug                    |  **Priority**   |     Minor      |
|      **Status**       |                 RESOLVED                 | **Resolution**  |     Fixed      |
| **Affects Version/s** | 3.0.26, 3.11.12, 4.0.1, 4.1, 4.1-alpha1  | **Component/s** | Tool/nodetool  |

### Description

I see the following warning trying to run nodetool upgradesstables

```
WARN  16:09:24 Only 34988 MB free across all data volumes. Consider adding more capacity to your cluster or removing obsolete snapshots
```

This warning is wrong because the wrong storage device capacity gets tested.

All my cassandra data paths are subdirectories of /data mount point which has enough of space:

```
$ df -h /data
Filesystem      Size  Used Avail Mounted on
.................     1.2T 200G  1T  /data
```

However what Warning reports is a OS mount which has nothing to do with Cassandra configuration:


```
df -h /
Filesystem      Size  Used Avail Use% Mounted on
............        40G  5.7G   35G  15% /
```

I see this error running Cassandra 3.0.22

### Testcase

TODO