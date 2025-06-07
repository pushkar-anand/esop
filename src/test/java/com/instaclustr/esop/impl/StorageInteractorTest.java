package com.instaclustr.esop.impl;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.nio.file.Path;
import java.nio.file.Paths;
import static org.testng.Assert.assertEquals;

public class StorageInteractorTest {

    // Minimal concrete subclass for testing
    static class TestStorageInteractor extends StorageInteractor {
        public TestStorageInteractor(StorageLocation storageLocation) {
            super(storageLocation);
        }

        @Override
        public RemoteObjectReference objectKeyToRemoteReference(Path objectKey) throws Exception {
            throw new UnsupportedOperationException("Not implemented for this test.");
        }

        @Override
        public RemoteObjectReference objectKeyToNodeAwareRemoteReference(Path objectKey) throws Exception {
            throw new UnsupportedOperationException("Not implemented for this test.");
        }

        @Override
        protected void cleanup() throws Exception {
            // No-op
        }
    }

    @DataProvider(name = "pathResolutionData")
    public Object[][] pathResolutionDataProvider() {
        return new Object[][]{
                // Storage Location String, Object Key String, Expected Resolved Path
                {"s3://bucket/cluster/dc/node", "manifests/manifest1.json", "cluster/dc/node/manifests/manifest1.json"},
                {"s3://bucket/prefix/cluster/dc/node", "manifests/manifest2.json", "prefix/cluster/dc/node/manifests/manifest2.json"},
                {"s3://bucket/p1/p2/cluster/dc/node", "data/ks/table/file.db", "p1/p2/cluster/dc/node/data/ks/table/file.db"},
                {"s3://bucket/cluster/dc/node", "some_file.txt", "cluster/dc/node/some_file.txt"},
                {"s3://bucket/prefix/cluster/dc/node", "another_file.txt", "prefix/cluster/dc/node/another_file.txt"},
                // Test with objectKey that might have leading slash (should be handled)
                {"s3://bucket/prefix/cluster/dc/node", "/leading/slash/file.txt", "prefix/cluster/dc/node/leading/slash/file.txt"},
                // Test with StorageLocation having a trailing slash (should be handled by StorageLocation constructor)
                {"s3://bucket/prefix/cluster/dc/node/", "file.txt", "prefix/cluster/dc/node/file.txt"},
                // Test with empty object key - should return nodePath itself
                {"s3://bucket/prefix/cluster/dc/node", "", "prefix/cluster/dc/node"},
                {"s3://bucket/cluster/dc/node", "", "cluster/dc/node"},
        };
    }

    @Test(dataProvider = "pathResolutionData")
    public void testResolveNodeAwareRemotePath(String slStr, String objectKeyStr, String expectedPath) {
        StorageLocation storageLocation = new StorageLocation(slStr);
        StorageInteractor interactor = new TestStorageInteractor(storageLocation);
        Path objectKey = Paths.get(objectKeyStr);
        String actualPath = interactor.resolveNodeAwareRemotePath(objectKey);
        assertEquals(actualPath, expectedPath);
    }

    @DataProvider(name = "pathResolutionDataForNodePathOnly")
    public Object[][] pathResolutionDataProviderNodePathOnly() {
         // Test cases where storageLocation.nodePath() itself might be what we want to test against if objectKey is empty
        return new Object[][]{
            // Test with prefix-only storage location (where cluster/dc/node might be null)
            // Current StorageLocation parsing for "s3://bucket/prefixonly" makes userDefinedPrefix="prefixonly", nodePath="prefixonly/null/null/null"
            {"s3://bucket/prefixonly", "manifest.json", "prefixonly/null/null/null/manifest.json"},
            {"s3://bucket/prefixonly", "", "prefixonly/null/null/null"},
        };
    }

    @Test(dataProvider = "pathResolutionDataForNodePathOnly")
    public void testResolveNodeAwareRemotePathWithPrefixOnlyLocation(String slStr, String objectKeyStr, String expectedPath) {
        StorageLocation storageLocation = new StorageLocation(slStr);
        StorageInteractor interactor = new TestStorageInteractor(storageLocation);
        Path objectKey = Paths.get(objectKeyStr);
        String actualPath = interactor.resolveNodeAwareRemotePath(objectKey);
        assertEquals(actualPath, expectedPath);
    }
}
