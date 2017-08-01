package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Created by cwc on 2017/07/29 0029.
 */
@RunWith(JUnit4.class)
public class FSNamesystemTest {
    @Test
    public void testNetworkTopology() {
        DatanodeDescriptor rack1_data1 = new DatanodeDescriptor(new DatanodeID("rack1_data1"), "/rack1/data1", "localhost");
        DatanodeDescriptor rack1_data2 = new DatanodeDescriptor(new DatanodeID("rack1_data2"), "/rack1/data2", "localhost");
        DatanodeDescriptor rack1_data3 = new DatanodeDescriptor(new DatanodeID("rack1_data3"), "/rack1/data3", "localhost");
        DatanodeDescriptor rack2_data1 = new DatanodeDescriptor(new DatanodeID("rack2_data1"), "/rack2/data1", "localhost");
        DatanodeDescriptor rack2_data2 = new DatanodeDescriptor(new DatanodeID("rack2_data2"), "/rack2/data2", "localhost");

        NetworkTopology networkTopology = new NetworkTopology();

        networkTopology.add(rack1_data1);
        networkTopology.add(rack1_data2);
        networkTopology.add(rack1_data3);
        networkTopology.add(rack2_data1);
        networkTopology.add(rack2_data2);

        Assert.assertEquals(4, networkTopology.getDistance(rack2_data1, rack2_data2));
        Assert.assertEquals(6, networkTopology.getDistance(rack1_data1, rack2_data2));

        Assert.assertEquals("/rack2/data2", networkTopology.getNode("/rack2/data2").getNetworkLocation());

    }

    public static void main(String[] args) {

    }
}