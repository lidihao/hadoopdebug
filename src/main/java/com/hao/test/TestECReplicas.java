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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Time;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TestECReplicas {
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
    private final String[] hosts = getHosts(dataBlocks + parityBlocks );
    private final String[] racks =
            getRacks(dataBlocks + parityBlocks, dataBlocks);

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
     * When there are all the internal blocks available but they are not placed on
     * enough racks, NameNode should avoid normal decoding reconstruction but copy
     * an internal block to a new rack.
     * <p>
     * In this test, we first need to create a scenario that a striped block has
     * all the internal blocks but distributed in <6 racks. Then we check if the
     * redundancy monitor can correctly schedule the reconstruction work for it.
     */
    @Test
    public void testReconstructForNotEnoughRacks() throws Exception {
        LOG.info("cluster hosts: {}, racks: {}", Arrays.asList(hosts),
                Arrays.asList(racks));
        cluster = new MiniDFSCluster.Builder(conf).racks(racks).hosts(hosts)
                .numDataNodes(hosts.length).build();
        cluster.waitActive();
        fs = cluster.getFileSystem();
        fs.enableErasureCodingPolicy(
                StripedFileTestUtil.getDefaultECPolicy().getName());
        fs.setErasureCodingPolicy(new Path("/"),
                StripedFileTestUtil.getDefaultECPolicy().getName());
        FSNamesystem fsn = cluster.getNamesystem();
        BlockManager bm = fsn.getBlockManager();
        DatanodeManager datanodeManager = bm.getDatanodeManager();
        NetworkTopology topology1 = bm.getDatanodeManager().getNetworkTopology();
        LOG.info("topology is: {}", topology1);
        DatanodeDescriptor dnd3 = datanodeManager.getDatanode(
                cluster.getDataNodes().get(3).getDatanodeId());
        final Path file = new Path("/foo");
        DFSTestUtil.createFile(fs, file,
                cellSize * dataBlocks * 2, (short) 1, 0L);
        System.err.println(bm.numOfUnderReplicatedBlocks());
        GenericTestUtils.waitFor(() ->
                bm.numOfUnderReplicatedBlocks() == 0, 100, 30000);
        LOG.info("Created file {}", file);

        datanodeManager.getDatanodeAdminManager().startDecommission(dnd3);
        TimeUnit.SECONDS.sleep(3);

        fsn.writeLock();
        try {
            bm.processMisReplicatedBlocks();
            while (true) {

                System.err.println("------------------------------" + bm.getLowRedundancyBlocks());
                TimeUnit.SECONDS.sleep(1);
            }
        } finally {
            fsn.writeUnlock();
        }
    }
}