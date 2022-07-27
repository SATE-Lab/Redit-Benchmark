# Redit-Kafka-13937

### Details

Title: StandardAuthorizer throws "ID 5t1jQ3zWSfeVLMYkN3uong not found in aclsById" exceptions into broker logs

|         Label         |       Value        |      Label       |   Value   |
|:---------------------:|:------------------:|:----------------:|:---------:|
|       **Type**        |        Bug         |   **Priority**   |   Major   |
|      **Status**       |      RESOLVED      |  **Resolution**  | Duplicate |
| **Affects Version/s** |       3.2.0        | **Component/s**  |   None    |

### Description

I'm trying to use the new StandardAuthorizer in a Kafka cluster running in KRaft mode. When managing the ACLs using the Admin API, the authorizer seems to throw a lot of runtime exceptions in the log. For example ...

When creating an ACL rule, it seems to create it just fine. But it throws the following exception:

```
2022-05-25 11:09:18,074 ERROR [StandardAuthorizer 0] addAcl error (org.apache.kafka.metadata.authorizer.StandardAuthorizerData) [EventHandler]
java.lang.RuntimeException: An ACL with ID 5t1jQ3zWSfeVLMYkN3uong already exists.
    at org.apache.kafka.metadata.authorizer.StandardAuthorizerData.addAcl(StandardAuthorizerData.java:169)
    at org.apache.kafka.metadata.authorizer.StandardAuthorizer.addAcl(StandardAuthorizer.java:83)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$19(BrokerMetadataPublisher.scala:234)
    at java.base/java.util.LinkedHashMap$LinkedEntrySet.forEach(LinkedHashMap.java:671)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18(BrokerMetadataPublisher.scala:232)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18$adapted(BrokerMetadataPublisher.scala:221)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataPublisher.publish(BrokerMetadataPublisher.scala:221)
    at kafka.server.metadata.BrokerMetadataListener.kafka$server$metadata$BrokerMetadataListener$$publish(BrokerMetadataListener.scala:258)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2(BrokerMetadataListener.scala:119)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2$adapted(BrokerMetadataListener.scala:119)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.run(BrokerMetadataListener.scala:119)
    at org.apache.kafka.queue.KafkaEventQueue$EventContext.run(KafkaEventQueue.java:121)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.handleEvents(KafkaEventQueue.java:200)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.run(KafkaEventQueue.java:173)
    at java.base/java.lang.Thread.run(Thread.java:829)
2022-05-25 11:09:18,076 ERROR [BrokerMetadataPublisher id=0] Error publishing broker metadata at OffsetAndEpoch(offset=3, epoch=1) (kafka.server.metadata.BrokerMetadataPublisher) [EventHandler]
java.lang.RuntimeException: An ACL with ID 5t1jQ3zWSfeVLMYkN3uong already exists.
    at org.apache.kafka.metadata.authorizer.StandardAuthorizerData.addAcl(StandardAuthorizerData.java:169)
    at org.apache.kafka.metadata.authorizer.StandardAuthorizer.addAcl(StandardAuthorizer.java:83)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$19(BrokerMetadataPublisher.scala:234)
    at java.base/java.util.LinkedHashMap$LinkedEntrySet.forEach(LinkedHashMap.java:671)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18(BrokerMetadataPublisher.scala:232)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18$adapted(BrokerMetadataPublisher.scala:221)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataPublisher.publish(BrokerMetadataPublisher.scala:221)
    at kafka.server.metadata.BrokerMetadataListener.kafka$server$metadata$BrokerMetadataListener$$publish(BrokerMetadataListener.scala:258)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2(BrokerMetadataListener.scala:119)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2$adapted(BrokerMetadataListener.scala:119)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.run(BrokerMetadataListener.scala:119)
    at org.apache.kafka.queue.KafkaEventQueue$EventContext.run(KafkaEventQueue.java:121)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.handleEvents(KafkaEventQueue.java:200)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.run(KafkaEventQueue.java:173)
    at java.base/java.lang.Thread.run(Thread.java:829)
2022-05-25 11:09:18,077 ERROR [BrokerMetadataListener id=0] Unexpected error handling HandleCommitsEvent (kafka.server.metadata.BrokerMetadataListener) [EventHandler]
java.lang.RuntimeException: An ACL with ID 5t1jQ3zWSfeVLMYkN3uong already exists.
    at org.apache.kafka.metadata.authorizer.StandardAuthorizerData.addAcl(StandardAuthorizerData.java:169)
    at org.apache.kafka.metadata.authorizer.StandardAuthorizer.addAcl(StandardAuthorizer.java:83)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$19(BrokerMetadataPublisher.scala:234)
    at java.base/java.util.LinkedHashMap$LinkedEntrySet.forEach(LinkedHashMap.java:671)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18(BrokerMetadataPublisher.scala:232)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18$adapted(BrokerMetadataPublisher.scala:221)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataPublisher.publish(BrokerMetadataPublisher.scala:221)
    at kafka.server.metadata.BrokerMetadataListener.kafka$server$metadata$BrokerMetadataListener$$publish(BrokerMetadataListener.scala:258)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2(BrokerMetadataListener.scala:119)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2$adapted(BrokerMetadataListener.scala:119)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.run(BrokerMetadataListener.scala:119)
    at org.apache.kafka.queue.KafkaEventQueue$EventContext.run(KafkaEventQueue.java:121)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.handleEvents(KafkaEventQueue.java:200)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.run(KafkaEventQueue.java:173)
    at java.base/java.lang.Thread.run(Thread.java:829) 

```

However, when I describe the ACL rules (again using the Admin API), they seem to be created and seem to work fine despite these errors.

Similarly, deleting the ACLs throws similar exceptions:

```
2022-05-25 11:10:04,261 ERROR [StandardAuthorizer 0] removeAcl error (org.apache.kafka.metadata.authorizer.StandardAuthorizerData) [EventHandler]
java.lang.RuntimeException: ID 5t1jQ3zWSfeVLMYkN3uong not found in aclsById.
    at org.apache.kafka.metadata.authorizer.StandardAuthorizerData.removeAcl(StandardAuthorizerData.java:189)
    at org.apache.kafka.metadata.authorizer.StandardAuthorizer.removeAcl(StandardAuthorizer.java:88)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$19(BrokerMetadataPublisher.scala:236)
    at java.base/java.util.LinkedHashMap$LinkedEntrySet.forEach(LinkedHashMap.java:671)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18(BrokerMetadataPublisher.scala:232)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18$adapted(BrokerMetadataPublisher.scala:221)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataPublisher.publish(BrokerMetadataPublisher.scala:221)
    at kafka.server.metadata.BrokerMetadataListener.kafka$server$metadata$BrokerMetadataListener$$publish(BrokerMetadataListener.scala:258)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2(BrokerMetadataListener.scala:119)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2$adapted(BrokerMetadataListener.scala:119)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.run(BrokerMetadataListener.scala:119)
    at org.apache.kafka.queue.KafkaEventQueue$EventContext.run(KafkaEventQueue.java:121)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.handleEvents(KafkaEventQueue.java:200)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.run(KafkaEventQueue.java:173)
    at java.base/java.lang.Thread.run(Thread.java:829)
2022-05-25 11:10:04,261 ERROR [BrokerMetadataPublisher id=0] Error publishing broker metadata at OffsetAndEpoch(offset=4, epoch=1) (kafka.server.metadata.BrokerMetadataPublisher) [EventHandler]
java.lang.RuntimeException: ID 5t1jQ3zWSfeVLMYkN3uong not found in aclsById.
    at org.apache.kafka.metadata.authorizer.StandardAuthorizerData.removeAcl(StandardAuthorizerData.java:189)
    at org.apache.kafka.metadata.authorizer.StandardAuthorizer.removeAcl(StandardAuthorizer.java:88)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$19(BrokerMetadataPublisher.scala:236)
    at java.base/java.util.LinkedHashMap$LinkedEntrySet.forEach(LinkedHashMap.java:671)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18(BrokerMetadataPublisher.scala:232)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18$adapted(BrokerMetadataPublisher.scala:221)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataPublisher.publish(BrokerMetadataPublisher.scala:221)
    at kafka.server.metadata.BrokerMetadataListener.kafka$server$metadata$BrokerMetadataListener$$publish(BrokerMetadataListener.scala:258)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2(BrokerMetadataListener.scala:119)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2$adapted(BrokerMetadataListener.scala:119)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.run(BrokerMetadataListener.scala:119)
    at org.apache.kafka.queue.KafkaEventQueue$EventContext.run(KafkaEventQueue.java:121)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.handleEvents(KafkaEventQueue.java:200)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.run(KafkaEventQueue.java:173)
    at java.base/java.lang.Thread.run(Thread.java:829)
2022-05-25 11:10:04,261 ERROR [BrokerMetadataListener id=0] Unexpected error handling HandleCommitsEvent (kafka.server.metadata.BrokerMetadataListener) [EventHandler]
java.lang.RuntimeException: ID 5t1jQ3zWSfeVLMYkN3uong not found in aclsById.
    at org.apache.kafka.metadata.authorizer.StandardAuthorizerData.removeAcl(StandardAuthorizerData.java:189)
    at org.apache.kafka.metadata.authorizer.StandardAuthorizer.removeAcl(StandardAuthorizer.java:88)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$19(BrokerMetadataPublisher.scala:236)
    at java.base/java.util.LinkedHashMap$LinkedEntrySet.forEach(LinkedHashMap.java:671)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18(BrokerMetadataPublisher.scala:232)
    at kafka.server.metadata.BrokerMetadataPublisher.$anonfun$publish$18$adapted(BrokerMetadataPublisher.scala:221)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataPublisher.publish(BrokerMetadataPublisher.scala:221)
    at kafka.server.metadata.BrokerMetadataListener.kafka$server$metadata$BrokerMetadataListener$$publish(BrokerMetadataListener.scala:258)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2(BrokerMetadataListener.scala:119)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.$anonfun$run$2$adapted(BrokerMetadataListener.scala:119)
    at scala.Option.foreach(Option.scala:437)
    at kafka.server.metadata.BrokerMetadataListener$HandleCommitsEvent.run(BrokerMetadataListener.scala:119)
    at org.apache.kafka.queue.KafkaEventQueue$EventContext.run(KafkaEventQueue.java:121)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.handleEvents(KafkaEventQueue.java:200)
    at org.apache.kafka.queue.KafkaEventQueue$EventHandler.run(KafkaEventQueue.java:173)
    at java.base/java.lang.Thread.run(Thread.java:829) 
```

Again, it seems to work fine and the ACL rules are deleted despite the exceptions being thrown.

This behaviour seems to be both with single node cluster as well as with 3 node clusters (in both cases all nodes had both controller and broker roles).

Reproducing this seems to be easy in my case with a very simple Admin API code:

```
KafkaPrincipal principal = new KafkaPrincipal("User", "my-user");

// Create ACL
AclBinding acl = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
        new AccessControlEntry(principal.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW)
);

admin.createAcls(List.of(acl)).all().get();

// Delete ACL
AclBindingFilter acl = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
        new AccessControlEntryFilter(principal.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW)
);

admin.deleteAcls(List.of(acl)).all().get(); 
```


### Testcase

TODO