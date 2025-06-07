package com.instaclustr.esop.backup;

import java.nio.file.Paths;

import com.instaclustr.esop.impl.StorageLocation;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class StorageLocationTest {

    @Test
    public void updateStorageLocationDatacenterTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        StorageLocation changedNode = StorageLocation.updateNodeId(storageLocation, "node2");
        StorageLocation changedDc = StorageLocation.updateDatacenter(changedNode, "dc2");

        assertEquals(changedDc.datacenterId, "dc2");
        assertEquals(changedDc.nodeId, "node2");
    }

    @Test
    public void updateStorageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/global");

        StorageLocation updatedLocation = StorageLocation.updateNodeId(storageLocation, "node2");

        assertEquals(updatedLocation.nodeId, "node2");
    }

    @Test
    public void storageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        storageLocation.validate();

        assertEquals(storageLocation.storageProvider, "gcp");
        assertEquals(storageLocation.bucket, "bucket");
        assertEquals(storageLocation.clusterId, "cluster");
        assertEquals(storageLocation.datacenterId, "dc");
        assertEquals(storageLocation.nodeId, "node");
    }

    @Test
    public void fileLocationTest() {
        StorageLocation fileLocation = new StorageLocation("file:///some/path/bucket/cluster/dc/node");

        fileLocation.validate();

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), "/some/path");
        assertEquals(fileLocation.bucket, "bucket");
        assertEquals(fileLocation.clusterId, "cluster");
        assertEquals(fileLocation.datacenterId, "dc");
        assertEquals(fileLocation.nodeId, "node");
    }

    @Test
    public void fileLocationTest2() {
        StorageLocation fileLocation = new StorageLocation("file:///tmp/a/b/c/d/");

        fileLocation.validate();

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), "/tmp");
        assertEquals(fileLocation.bucket, "a");
        assertEquals(fileLocation.clusterId, "b");
        assertEquals(fileLocation.datacenterId, "c");
        assertEquals(fileLocation.nodeId, "d");
    }

    @Test
    public void fileLocationTest3() {
        StorageLocation fileLocation = new StorageLocation("file:///a/b/c/d/");

        fileLocation.validate();

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), Paths.get("").toAbsolutePath().toString());
        assertEquals(fileLocation.bucket, "a");
        assertEquals(fileLocation.clusterId, "b");
        assertEquals(fileLocation.datacenterId, "c");
        assertEquals(fileLocation.nodeId, "d");
    }

    @Test
    public void globalLocationTest() {
        StorageLocation globalLocation = new StorageLocation("oracle://my-bucket");

        globalLocation.validate();

        assertEquals(globalLocation.storageProvider, "oracle");
        assertEquals(globalLocation.bucket, "my-bucket");
        assertNull(globalLocation.clusterId);
        assertNull(globalLocation.datacenterId);
        assertNull(globalLocation.nodeId);
        assertTrue(globalLocation.cloudLocation);
        assertTrue(globalLocation.globalRequest);
    }

    @Test
    public void updateGlobalLocationTest() {
        StorageLocation globalLocation = new StorageLocation("oracle://my-bucket");

        StorageLocation updated = StorageLocation.update(globalLocation, "clusterName", "datacenterId", "nodeId");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName");
        assertEquals(updated.datacenterId, "datacenterId");
        assertEquals(updated.nodeId, "nodeId");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateNodeIdLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateNodeId(location, "nodeId2");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName");
        assertEquals(updated.datacenterId, "datacenterId");
        assertEquals(updated.nodeId, "nodeId2");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateDatacenterIdLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateDatacenter(location, "datacenterId2");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName");
        assertEquals(updated.datacenterId, "datacenterId2");
        assertEquals(updated.nodeId, "nodeId");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateClusterNameLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateClusterName(location, "clusterName2");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName2");
        assertEquals(updated.datacenterId, "datacenterId");
        assertEquals(updated.nodeId, "nodeId");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void cloudLocationWithSingleSegmentPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://mybucket/customprefix/clusterA/dc1/node1");
        sl.validate();
        assertEquals(sl.storageProvider, "s3");
        assertEquals(sl.bucket, "mybucket");
        assertEquals(sl.userDefinedPrefix, "customprefix");
        assertEquals(sl.clusterId, "clusterA");
        assertEquals(sl.datacenterId, "dc1");
        assertEquals(sl.nodeId, "node1");
        assertFalse(sl.globalRequest);
        assertEquals(sl.nodePath(), "customprefix/clusterA/dc1/node1");
    }

    @Test
    public void cloudLocationWithMultiSegmentPrefixTest() {
        StorageLocation sl = new StorageLocation("gcp://yourbucket/level1/level2/clusterB/dc2/node2");
        sl.validate();
        assertEquals(sl.storageProvider, "gcp");
        assertEquals(sl.bucket, "yourbucket");
        assertEquals(sl.userDefinedPrefix, "level1/level2");
        assertEquals(sl.clusterId, "clusterB");
        assertEquals(sl.datacenterId, "dc2");
        assertEquals(sl.nodeId, "node2");
        assertFalse(sl.globalRequest);
        assertEquals(sl.nodePath(), "level1/level2/clusterB/dc2/node2");
    }

    @Test
    public void cloudLocationWithPrefixOnlyTest() {
        StorageLocation sl = new StorageLocation("azure://blobcontainer/prefixonly/path");
        sl.validate();
        assertEquals(sl.storageProvider, "azure");
        assertEquals(sl.bucket, "blobcontainer");
        assertEquals(sl.userDefinedPrefix, "prefixonly/path");
        assertNull(sl.clusterId);
        assertNull(sl.datacenterId);
        assertNull(sl.nodeId);
        assertFalse(sl.globalRequest); // Path after bucket means not global for c/d/n determination
        assertEquals(sl.nodePath(), "prefixonly/path/null/null/null");
    }

    @Test
    public void cloudLocationNoPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://mybucket/clusterA/dc1/node1");
        sl.validate();
        assertEquals(sl.storageProvider, "s3");
        assertEquals(sl.bucket, "mybucket");
        assertNull(sl.userDefinedPrefix);
        assertEquals(sl.clusterId, "clusterA");
        assertEquals(sl.datacenterId, "dc1");
        assertEquals(sl.nodeId, "node1");
        assertFalse(sl.globalRequest);
        assertEquals(sl.nodePath(), "clusterA/dc1/node1");
    }

    @Test
    public void globalLocationWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://mybucket/someprefix/");
        sl.validate();
        assertEquals(sl.storageProvider, "s3");
        assertEquals(sl.bucket, "mybucket");
        assertEquals(sl.userDefinedPrefix, "someprefix");
        assertNull(sl.clusterId);
        assertNull(sl.datacenterId);
        assertNull(sl.nodeId);
        assertFalse(sl.globalRequest); // Path after bucket means not global for c/d/n determination
    }

    @Test
    public void globalLocationWithEmptyPathAfterBucketTest() {
        StorageLocation sl = new StorageLocation("s3://mybucket/");
        sl.validate();
        assertEquals(sl.storageProvider, "s3");
        assertEquals(sl.bucket, "mybucket");
        assertNull(sl.userDefinedPrefix);
        assertNull(sl.clusterId);
        assertNull(sl.datacenterId);
        assertNull(sl.nodeId);
        assertTrue(sl.globalRequest);
    }

    @Test
    public void nodePathTestWithoutPrefix() {
        StorageLocation sl = new StorageLocation("s3://bucket/cluster/dc/node");
        assertEquals(sl.nodePath(), "cluster/dc/node");
    }

    @Test
    public void nodePathTestWithPrefix() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/cluster/dc/node");
        assertEquals(sl.nodePath(), "prefix/cluster/dc/node");
    }

    @Test
    public void nodePathTestWithMultiSegmentPrefix() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/path/cluster/dc/node");
        assertEquals(sl.nodePath(), "prefix/path/cluster/dc/node");
    }

    @Test
    public void updateNodeIdWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/c1/d1/n1");
        StorageLocation updated = StorageLocation.updateNodeId(sl, "n2");
        assertEquals(updated.rawLocation, "s3://bucket/prefix/c1/d1/n2");
        assertEquals(updated.userDefinedPrefix, "prefix");
        assertEquals(updated.nodeId, "n2");
    }

    @Test
    public void updateDatacenterWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/c1/d1/n1");
        StorageLocation updated = StorageLocation.updateDatacenter(sl, "d2");
        assertEquals(updated.rawLocation, "s3://bucket/prefix/c1/d2/n1");
        assertEquals(updated.userDefinedPrefix, "prefix");
        assertEquals(updated.datacenterId, "d2");
    }

    @Test
    public void updateClusterNameWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/c1/d1/n1");
        StorageLocation updated = StorageLocation.updateClusterName(sl, "c2");
        assertEquals(updated.rawLocation, "s3://bucket/prefix/c2/d1/n1");
        assertEquals(updated.userDefinedPrefix, "prefix");
        assertEquals(updated.clusterId, "c2");
    }

    @Test
    public void updateFullLocationWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/customprefix"); // prefix-only initial location
        StorageLocation updated = StorageLocation.update(sl, "c1", "d1", "n1");
        assertEquals(updated.rawLocation, "s3://bucket/customprefix/c1/d1/n1");
        assertEquals(updated.userDefinedPrefix, "customprefix"); // This should be preserved
        assertEquals(updated.clusterId, "c1");
        assertEquals(updated.datacenterId, "d1");
        assertEquals(updated.nodeId, "n1");
    }

    @Test
    public void withoutNodeAndDcWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/c1/d1/n1");
        assertEquals(sl.withoutNodeAndDc(), "s3://bucket/prefix/c1");
    }

    @Test
    public void withoutNodeWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/c1/d1/n1");
        assertEquals(sl.withoutNode(), "s3://bucket/prefix/c1/d1");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void validateInvalidPathTooFewPartsForNodeWithPrefixTest() {
        StorageLocation sl = new StorageLocation("s3://bucket/prefix/cluster/dc"); // Missing node
        sl.validate(); // Should throw
    }
}
