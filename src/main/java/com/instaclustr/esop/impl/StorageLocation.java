package com.instaclustr.esop.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;

import static java.lang.String.format;

public class StorageLocation {

    private static final Pattern filePattern = Pattern.compile("(.*)://(.*)/(.*)/(.*)/(.*)/(.*)");
    private static final Pattern cloudPattern = Pattern.compile("(.*)://(.*)/(.*)/(.*)/(.*)");
    private static final Pattern globalPattern = Pattern.compile("(.*)://(.*)");

    public String rawLocation;
    public String storageProvider;
    public String bucket;
    public String clusterId;
    public String datacenterId;
    public String nodeId;
    public String userDefinedPrefix;
    public Path fileBackupDirectory;
    public boolean cloudLocation;
    public boolean globalRequest;

    public StorageLocation(final String rawLocation) {

        if (rawLocation.endsWith("/")) {
            this.rawLocation = rawLocation.substring(0, rawLocation.length() - 1);
        } else {
            this.rawLocation = rawLocation;
        }

        if (this.rawLocation.startsWith("file")) {
            initializeFileBackupLocation(this.rawLocation);
        } else {
            cloudLocation = true;
            initializeCloudLocation(this.rawLocation);
        }
    }

    private void initializeFileBackupLocation(final String backupLocation) {
        final Matcher matcher = filePattern.matcher(backupLocation);

        if (!matcher.matches()) {
            return;
        }

        this.rawLocation = matcher.group();
        this.storageProvider = matcher.group(1);
        this.fileBackupDirectory = Paths.get(matcher.group(2));
        this.bucket = matcher.group(3);
        this.clusterId = matcher.group(4);
        this.datacenterId = matcher.group(5);
        this.nodeId = matcher.group(6);

        if (fileBackupDirectory.toString().isEmpty()) {
            fileBackupDirectory = fileBackupDirectory.toAbsolutePath();
        }
    }

    private void initializeCloudLocation(final String storageLocation) {
        this.rawLocation = storageLocation; // Keep original raw location
        this.storageProvider = storageLocation.substring(0, storageLocation.indexOf("://"));
        cloudLocation = true;

        String pathWithoutProtocol = storageLocation.substring(storageLocation.indexOf("://") + 3);
        int firstSlash = pathWithoutProtocol.indexOf('/');

        if (firstSlash == -1) { // format like s3://mybucket (global request)
            this.bucket = pathWithoutProtocol;
            this.userDefinedPrefix = null;
            this.clusterId = null;
            this.datacenterId = null;
            this.nodeId = null;
            this.globalRequest = true;
        } else {
            this.bucket = pathWithoutProtocol.substring(0, firstSlash);
            String pathAfterBucket = pathWithoutProtocol.substring(firstSlash + 1);

            if (pathAfterBucket.endsWith("/")) {
                pathAfterBucket = pathAfterBucket.substring(0, pathAfterBucket.length() - 1);
            }

            if (pathAfterBucket.isEmpty()) {
                this.userDefinedPrefix = null;
                this.clusterId = null;
                this.datacenterId = null;
                this.nodeId = null;
                this.globalRequest = true;
            } else {
                String[] parts = pathAfterBucket.split("/");

                if (parts.length >= 3) {
                    this.nodeId = parts[parts.length - 1];
                    this.datacenterId = parts[parts.length - 2];
                    this.clusterId = parts[parts.length - 3];

                    if (parts.length > 3) {
                        this.userDefinedPrefix = String.join("/", java.util.Arrays.copyOfRange(parts, 0, parts.length - 3));
                    } else {
                        this.userDefinedPrefix = null;
                    }
                    this.globalRequest = false;
                } else {
                    this.userDefinedPrefix = pathAfterBucket;
                    this.clusterId = null;
                    this.datacenterId = null;
                    this.nodeId = null;
                    this.globalRequest = false;
                }
            }
        }

        if (this.userDefinedPrefix != null && this.userDefinedPrefix.isEmpty()) {
            this.userDefinedPrefix = null;
        }
    }

    public boolean incompleteNodeLocation() {
        return clusterId == null || datacenterId == null || nodeId == null;
    }

    public void validate() throws IllegalStateException {
        if (cloudLocation) {
            if (storageProvider == null || bucket == null) {
                throw new IllegalStateException("Cloud storage location must specify protocol and bucket.");
            }

            if (!globalRequest && nodeId == null && userDefinedPrefix == null) {
                throw new IllegalStateException(format("Storage location %s is incomplete for a node-specific operation and is not a valid prefix-level path. Path must be 'protocol://bucket', 'protocol://bucket/prefix', or 'protocol://bucket[/prefix]/cluster/dc/node'.",
                                                       rawLocation));
            }

        } else {
            if (rawLocation == null || storageProvider == null || bucket == null || clusterId == null || datacenterId == null || nodeId == null || fileBackupDirectory == null) {
                throw new IllegalStateException(format("File storage location %s is not in form file:///some/backup/path/bucket/clusterId/datacenterId/nodeId",
                                                       rawLocation));
            }
        }

        if (bucket != null && bucket.endsWith("/")) {
            throw new IllegalStateException(format("Wrong bucket name: %s (should not end with /)", bucket));
        }
        if (userDefinedPrefix != null && userDefinedPrefix.endsWith("/")) {
            throw new IllegalStateException(format("Wrong userDefinedPrefix: %s (should not end with /)", userDefinedPrefix));
        }
        if (clusterId != null && clusterId.endsWith("/")) {
            throw new IllegalStateException(format("Wrong cluster name: %s (should not end with /)", clusterId));
        }
        if (datacenterId != null && datacenterId.endsWith("/")) {
            throw new IllegalStateException(format("Wrong datacenter name: %s (should not end with /)", datacenterId));
        }
        if (nodeId != null && nodeId.endsWith("/")) {
            throw new IllegalStateException(format("Wrong node name: %s (should not end with /)", nodeId));
        }
    }

    public String nodePath() {
        if (clusterId == null || datacenterId == null || nodeId == null) {
            String c = (clusterId == null) ? "null" : clusterId;
            String dc = (datacenterId == null) ? "null" : datacenterId;
            String n = (nodeId == null) ? "null" : nodeId;
            String path = String.format("%s/%s/%s", c, dc, n);
            return (userDefinedPrefix != null ? userDefinedPrefix + "/" : "") + path;
        }
        return (userDefinedPrefix != null ? userDefinedPrefix + "/" : "") + String.format("%s/%s/%s", clusterId, datacenterId, nodeId);
    }

    public static StorageLocation updateNodeId(final StorageLocation oldLocation, String nodeId) {
        String newRawLocation;
        if (oldLocation.cloudLocation) {
            String base = oldLocation.storageProvider + "://" + oldLocation.bucket;
            if (oldLocation.userDefinedPrefix != null) {
                base += "/" + oldLocation.userDefinedPrefix;
            }
            String cluster = oldLocation.clusterId != null ? oldLocation.clusterId : "";
            String dc = oldLocation.datacenterId != null ? oldLocation.datacenterId : "";
            newRawLocation = base + "/" + cluster + "/" + dc + "/" + nodeId;
        } else {
            newRawLocation = oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/") + 1) + nodeId;
        }
        return new StorageLocation(newRawLocation);
    }

    public static StorageLocation updateDatacenter(final StorageLocation oldLocation, final String dc) {
        String newRawLocation;
        if (oldLocation.cloudLocation) {
            String base = oldLocation.storageProvider + "://" + oldLocation.bucket;
            if (oldLocation.userDefinedPrefix != null) {
                base += "/" + oldLocation.userDefinedPrefix;
            }
            String cluster = oldLocation.clusterId != null ? oldLocation.clusterId : "";
            String node = oldLocation.nodeId != null ? oldLocation.nodeId : "";
            newRawLocation = base + "/" + cluster + "/" + dc + "/" + node;
        } else {
            final String withoutNodeId = oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/"));
            final String withoutDatacenter = withoutNodeId.substring(0, withoutNodeId.lastIndexOf("/"));
            newRawLocation = withoutDatacenter + "/" + dc + "/" + oldLocation.nodeId;
        }
        return new StorageLocation(newRawLocation);
    }

    public static StorageLocation updateClusterName(final StorageLocation oldLocation, final String clusterName) {
        String newRawLocation;
        if (oldLocation.cloudLocation) {
            String base = oldLocation.storageProvider + "://" + oldLocation.bucket;
            if (oldLocation.userDefinedPrefix != null) {
                base += "/" + oldLocation.userDefinedPrefix;
            }
            String dc = oldLocation.datacenterId != null ? oldLocation.datacenterId : "";
            String node = oldLocation.nodeId != null ? oldLocation.nodeId : "";
            newRawLocation = base + "/" + clusterName + "/" + dc + "/" + node;
        } else {
            final String withoutNodeId = oldLocation.rawLocation.substring(0, oldLocation.rawLocation.lastIndexOf("/"));
            final String withoutDatacenter = withoutNodeId.substring(0, withoutNodeId.lastIndexOf("/"));
            final String withoutClusterName = withoutDatacenter.substring(0, withoutDatacenter.lastIndexOf("/"));
            newRawLocation = withoutClusterName + "/" + clusterName + "/" + oldLocation.datacenterId + "/" + oldLocation.nodeId;
        }
        return new StorageLocation(newRawLocation);
    }

    public static StorageLocation updateNodeId(final StorageLocation oldLocation, UUID nodeId) {
        return StorageLocation.updateNodeId(oldLocation, nodeId.toString());
    }

    public static StorageLocation update(final StorageLocation oldLocation, final String clusterName, final String datacenterId, final String hostId) {
        if (oldLocation.cloudLocation) {
            String base = oldLocation.storageProvider + "://" + oldLocation.bucket;
            if (oldLocation.userDefinedPrefix != null) {
                base += "/" + oldLocation.userDefinedPrefix;
            }
            return new StorageLocation(String.format("%s/%s/%s/%s", base, clusterName, datacenterId, hostId));
        } else {
            final StorageLocation tempForNodeUpdate = new StorageLocation(oldLocation.rawLocation);
            final StorageLocation updatedNodeId = updateNodeId(tempForNodeUpdate, hostId);
            final StorageLocation updatedDatacenter = updateDatacenter(updatedNodeId, datacenterId);
            return updateClusterName(updatedDatacenter, clusterName);
        }
    }

    public String withoutNode() {
        if (cloudLocation) {
            String base = storageProvider + "://" + bucket;
            if (userDefinedPrefix != null) {
                base += "/" + userDefinedPrefix;
            }
            if (clusterId != null && datacenterId != null) {
                return base + "/" + clusterId + "/" + datacenterId;
            } else if (clusterId != null) {
                return base + "/" + clusterId;
            } else {
                return base;
            }
        } else {
            return rawLocation.substring(0, rawLocation.lastIndexOf("/"));
        }
    }

    public String withoutNodeAndDc() {
        if (cloudLocation) {
            String base = storageProvider + "://" + bucket;
            if (userDefinedPrefix != null) {
                base += "/" + userDefinedPrefix;
            }
            if (clusterId != null) {
                return base + "/" + clusterId;
            } else {
                return base;
            }
        } else {
            final String withoutNode = rawLocation.substring(0, rawLocation.lastIndexOf("/"));
            return withoutNode.substring(0, withoutNode.lastIndexOf("/"));
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("rawLocation", rawLocation)
            .add("storageProvider", storageProvider)
            .add("bucket", bucket)
            .add("userDefinedPrefix", userDefinedPrefix)
            .add("clusterId", clusterId)
            .add("datacenterId", datacenterId)
            .add("nodeId", nodeId)
            .add("fileBackupDirectory", fileBackupDirectory)
            .add("cloudLocation", cloudLocation)
            .add("globalRequest", globalRequest)
            .toString();
    }

    public static class StorageLocationTypeConverter implements ITypeConverter<StorageLocation> {

        @Override
        public StorageLocation convert(final String value) throws Exception {
            if (value == null) {
                return null;
            }

            try {
                return new StorageLocation(value);
            } catch (final Exception ex) {
                throw new CommandLine.TypeConversionException(format("Invalid value of StorageLocation '%s', reason: %s",
                                                                     value,
                                                                     ex.getLocalizedMessage()));
            }
        }
    }

    public static class StorageLocationSerializer extends StdSerializer<StorageLocation> {

        public StorageLocationSerializer() {
            super(StorageLocation.class);
        }

        protected StorageLocationSerializer(final Class<StorageLocation> t) {
            super(t);
        }

        @Override
        public void serialize(final StorageLocation value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            if (value != null) {
                gen.writeString(value.rawLocation);
            }
        }
    }

    public static class StorageLocationDeserializer extends StdDeserializer<StorageLocation> {

        public StorageLocationDeserializer() {
            super(StorageLocation.class);
        }

        @Override
        public StorageLocation deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
            final String valueAsString = p.getValueAsString();

            if (valueAsString == null) {
                return null;
            }

            try {
                return new StorageLocation(valueAsString);
            } catch (final Exception ex) {
                throw new InvalidFormatException(p, "Invalid StorageLocation.", valueAsString, StorageLocation.class);
            }
        }
    }
}
