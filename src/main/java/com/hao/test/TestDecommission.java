package com.hao.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestDecommission {
    public static final Logger LOG = LoggerFactory.getLogger(
            org.apache.hadoop.hdfs.server.blockmanagement.TestReconstructStripedBlocksWithRackAwareness.class);

    static {
        GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG, Level.ALL);
        GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.ALL);
        GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    }

    private final ErasureCodingPolicy ecPolicy =
            StripedFileTestUtil.getDefaultECPolicy();
    private final int cellSize = ecPolicy.getCellSize();
    private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
    private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
    private final String[] hosts = getHosts(dataBlocks + parityBlocks + 1);
    private final String[] racks =
            getRacks(dataBlocks + parityBlocks + 1, dataBlocks);

    private static String[] getHosts(int numHosts) {
        String[] hosts = new String[numHosts];
        for (int i = 0; i < hosts.length; i++) {
            hosts[i] = "host" + (i + 1);
        }
        return hosts;
    }

    private static String[] getRacks(int numHosts, int numRacks) {
        String[] racks = new String[numHosts];
        int numHostEachRack = numHosts / numRacks;
        int residue = numHosts % numRacks;
        int j = 0;
        for (int i = 1; i <= numRacks; i++) {
            int limit = i <= residue ? numHostEachRack + 1 : numHostEachRack;
            for (int k = 0; k < limit; k++) {
                racks[j++] = "/r" + i;
            }
        }
        assert j == numHosts;
        return racks;
    }

    private MiniDFSCluster cluster;
    private static final HdfsConfiguration conf = new HdfsConfiguration();
    private DistributedFileSystem fs;

    @BeforeClass
    public static void setup() throws Exception {
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
                false);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 1);
    }

    @After
    public void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    private MiniDFSCluster.DataNodeProperties stopDataNode(String hostname)
            throws IOException {
        MiniDFSCluster.DataNodeProperties dnProp = null;
        for (int i = 0; i < cluster.getDataNodes().size(); i++) {
            DataNode dn = cluster.getDataNodes().get(i);
            if (dn.getDatanodeId().getHostName().equals(hostname)) {
                dnProp = cluster.stopDataNode(i);
                cluster.setDataNodeDead(dn.getDatanodeId());
                LOG.info("stop datanode " + dn.getDatanodeId().getHostName());
            }
        }
        return dnProp;
    }

    private DataNode getDataNode(String host) {
        for (DataNode dn : cluster.getDataNodes()) {
            if (dn.getDatanodeId().getHostName().equals(host)) {
                return dn;
            }
        }
        return null;
    }



    /**
     * In case we have 10 internal blocks on 5 racks, where 9 of blocks are live
     * and 1 decommissioning, make sure the reconstruction happens correctly.
     */
    @Test
    public void testReconstructionWithDecommission() throws Exception {
        final String[] rackNames = getRacks(dataBlocks + parityBlocks,
                dataBlocks);
        final String[] hostNames = getHosts(dataBlocks + parityBlocks);
        // we now have 11 hosts on 6 racks with distribution: 2-2-2-2-2-1
        cluster = new MiniDFSCluster.Builder(conf).racks(rackNames).hosts(hostNames)
                .numDataNodes(hostNames.length).build();
        cluster.waitActive();
        fs = cluster.getFileSystem();
        fs.enableErasureCodingPolicy(
                StripedFileTestUtil.getDefaultECPolicy().getName());
        fs.setErasureCodingPolicy(new Path("/"),
                StripedFileTestUtil.getDefaultECPolicy().getName());

        final BlockManager bm = cluster.getNamesystem().getBlockManager();
        final DatanodeManager dm = bm.getDatanodeManager();

        final Path file = new Path("/foo");
        DFSTestUtil.createFile(fs, file,
                cellSize * dataBlocks * 2, (short) 1, 0L);
        final BlockInfo blockInfo = cluster.getNamesystem().getFSDirectory()
                .getINode(file.toString()).asFile().getLastBlock();
        // stop h9 and h10 and create a file with 6+3 internal blocks
        MiniDFSCluster.DataNodeProperties h9 =
                stopDataNode(hostNames[hostNames.length - 3]);
        MiniDFSCluster.DataNodeProperties h10 =
                stopDataNode(hostNames[hostNames.length - 2]);
        // bring h9 back
        //cluster.restartDataNode(h9);
        // cluster.waitActive();

        // stop h11 so that the reconstruction happens
        //MiniDFSCluster.DataNodeProperties h11 =
        //        stopDataNode(hostNames[hostNames.length - 1]);
        NetworkTopology topology1 = bm.getDatanodeManager().getNetworkTopology();
        LOG.info("topology is: {}", topology1);
        boolean recovered = bm.countNodes(blockInfo).liveReplicas() >=
                dataBlocks + parityBlocks;
        while (!recovered) {
            System.err.println(bm.countNodes(blockInfo).liveReplicas()+"__________" + recovered + "  " + bm.numOfUnderReplicatedBlocks());
            Thread.sleep(1000);
            recovered = bm.countNodes(blockInfo).liveReplicas() >=
                    dataBlocks + parityBlocks;
        }
    }
}

