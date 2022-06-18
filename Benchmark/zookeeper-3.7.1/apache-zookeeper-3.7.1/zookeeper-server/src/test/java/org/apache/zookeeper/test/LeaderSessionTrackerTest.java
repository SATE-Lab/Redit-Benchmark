/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Due to race condition or bad client code, the leader may get request from
 * expired session. We need to make sure that we never allow ephmeral node
 * to be created in those case, but we do allow normal node to be created.
 */
public class LeaderSessionTrackerTest extends ZKTestCase {

    protected static final Logger LOG = LoggerFactory.getLogger(LeaderSessionTrackerTest.class);

    QuorumUtil qu;

    @BeforeEach
    public void setUp() throws Exception {
        qu = new QuorumUtil(1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        qu.shutdownAll();
    }

    @Test
    public void testExpiredSessionWithLocalSession() throws Exception {
        testCreateEphemeral(true);
    }

    @Test
    public void testExpiredSessionWithoutLocalSession() throws Exception {
        testCreateEphemeral(false);
    }

    /**
     * When we create ephemeral node, we need to check against global
     * session, so the leader never accept request from an expired session
     * (that we no longer track)
     *
     * This is not the same as SessionInvalidationTest since session
     * is not in closing state
     */
    public void testCreateEphemeral(boolean localSessionEnabled) throws Exception {
        if (localSessionEnabled) {
            qu.enableLocalSession(true);
        }
        qu.startAll();
        QuorumPeer leader = qu.getLeaderQuorumPeer();

        ZooKeeper zk = ClientBase.createZKClient(qu.getConnectString(leader));

        CreateRequest createRequest = new CreateRequest("/impossible", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL.toFlag());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        createRequest.serialize(boa, "request");
        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

        // Mimic sessionId generated by follower's local session tracker
        long sid = qu.getFollowerQuorumPeers().get(0).getActiveServer().getServerId();
        long fakeSessionId = (sid << 56) + 1;

        LOG.info("Fake session Id: {}", Long.toHexString(fakeSessionId));

        Request request = new Request(null, fakeSessionId, 0, OpCode.create, bb, new ArrayList<Id>());

        // Submit request directly to leader
        leader.getActiveServer().submitRequest(request);

        // Make sure that previous request is finished
        zk.create("/ok", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Stat stat = zk.exists("/impossible", null);
        assertEquals(null, stat, "Node from fake session get created");

    }

    /**
     * When local session is enabled, leader will allow persistent node
     * to be create for unknown session
     */
    @Test
    public void testCreatePersistent() throws Exception {
        qu.enableLocalSession(true);
        qu.startAll();

        QuorumPeer leader = qu.getLeaderQuorumPeer();

        ZooKeeper zk = ClientBase.createZKClient(qu.getConnectString(leader));

        CreateRequest createRequest = new CreateRequest("/success", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT.toFlag());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        createRequest.serialize(boa, "request");
        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

        // Mimic sessionId generated by follower's local session tracker
        long sid = qu.getFollowerQuorumPeers().get(0).getActiveServer().getServerId();
        long locallSession = (sid << 56) + 1;

        LOG.info("Local session Id: {}", Long.toHexString(locallSession));

        Request request = new Request(null, locallSession, 0, OpCode.create, bb, new ArrayList<Id>());

        // Submit request directly to leader
        leader.getActiveServer().submitRequest(request);

        // Make sure that previous request is finished
        zk.create("/ok", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Stat stat = zk.exists("/success", null);
        assertTrue(stat != null, "Request from local sesson failed");

    }

}
