package com.instaclustr.esop.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.local.LocalFileRestorer;

public abstract class StorageInteractor implements AutoCloseable {

    protected StorageLocation storageLocation;
    protected LocalFileRestorer localFileRestorer;

    public abstract RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception;

    public abstract RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception;

    public Path resolveRoot() {
        return Paths.get("/");
    }

    public StorageInteractor(final StorageLocation storageLocation) {
        this.storageLocation = storageLocation;
    }

    public String resolveNodeAwareRemoteRoot() {
        return Paths.get(storageLocation.clusterId).resolve(storageLocation.datacenterId).resolve(storageLocation.nodeId).toString();
    }

    public String resolveNodeAwareRemotePath(final Path objectKey) {
        // storageLocation.nodePath() gives us "[userDefinedPrefix]/clusterId/datacenterId/nodeId"
        // objectKey is "manifests/autosnap-123123.json" or "data/ks/table/file.db"
        // The result should be "[userDefinedPrefix]/clusterId/datacenterId/nodeId/manifests/autosnap-123123.json"
        // or "[userDefinedPrefix]/clusterId/datacenterId/nodeId/data/ks/table/file.db"
        String remoteObjectKey = objectKey.toString();
        // Ensure we don't have leading slashes from objectKey.toString() if it's an absolute path by mistake
        if (remoteObjectKey.startsWith("/")) {
            remoteObjectKey = remoteObjectKey.substring(1);
        }

        String basePath = storageLocation.nodePath();
        // Ensure basePath does not end with a slash if remoteObjectKey is empty or starts with one (after stripping)
        // This logic aims to prevent double slashes, e.g. "prefix//key" or "prefix/key/" if key is empty
        if (basePath.endsWith("/")) {
            if (remoteObjectKey.isEmpty() || remoteObjectKey.startsWith("/")) { // Should not happen due to stripping above but defensive
                basePath = basePath.substring(0, basePath.length() - 1);
            }
        }

        // If remoteObjectKey is empty, we want "basePath", not "basePath/"
        if (remoteObjectKey.isEmpty()) {
            return basePath;
        }

        return basePath + "/" + remoteObjectKey;
    }

    public List<Manifest> listManifests() throws Exception {
        throw new UnsupportedOperationException();
    }

    public void deleteNodeAwareKey(final Path objectKey) throws Exception {
        delete(objectKey, true);
    }

    public void delete(final Path objectKey, boolean nodeAware) throws Exception {
        throw new UnsupportedOperationException();
    }

    public void delete(final ManifestReport report, final RemoveBackupRequest request) throws Exception {
        throw new UnsupportedOperationException();
    }

    public List<StorageLocation> listNodes() throws Exception {
        throw new UnsupportedOperationException();
    }

    public List<StorageLocation> listNodes(final String dc) throws Exception {
        throw new UnsupportedOperationException();
    }

    public List<StorageLocation> listNodes(final List<String> dcs) throws Exception {
        throw new UnsupportedOperationException();
    }

    public List<String> listDcs() throws Exception {
        throw new UnsupportedOperationException();
    }

    public void deleteTopology(final String name) throws Exception {
        // name is typically the snapshotTag
        String prefix = this.storageLocation.userDefinedPrefix;
        Path topologyFilePath;
        Path basePath = Paths.get("topology"); // "topology"

        if (prefix != null && !prefix.isEmpty()) {
            String cleanPrefix = prefix.startsWith("/") ? prefix.substring(1) : prefix;
            cleanPrefix = cleanPrefix.endsWith("/") ? cleanPrefix.substring(0, cleanPrefix.length() - 1) : cleanPrefix;

            if (cleanPrefix.isEmpty()) {
                topologyFilePath = basePath.resolve(name + ".json"); // "topology/name.json"
            } else {
                // Constructs "userDefinedPrefix/topology/name.json"
                topologyFilePath = Paths.get(cleanPrefix).resolve(basePath).resolve(name + ".json");
            }
        } else {
            topologyFilePath = basePath.resolve(name + ".json"); // "topology/name.json"
        }

        // The 'delete' method with 'nodeAware == false' expects a path relative to the bucket root.
        // The Restorer implementations of delete(path, false) use objectKeyToRemoteReference.
        // objectKeyToRemoteReference takes the given path as the canonical path.
        // So, topologyFilePath (e.g., "userPrefix/topology/name.json") is correct here.
        delete(topologyFilePath, false);
    }

    public StorageLocation getStorageLocation() {
        return this.storageLocation;
    }

    public void update(final StorageLocation storageLocation,
                       final LocalFileRestorer restorer) {
        this.storageLocation = storageLocation;
        this.localFileRestorer = restorer;
    }

    protected abstract void cleanup() throws Exception;

    private boolean isClosed = false;

    public void init(List<ManifestEntry> manifestEntries) {}

    public void close() throws IOException {
        if (isClosed) {
            return;
        }

        try {
            cleanup();

            isClosed = true;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
