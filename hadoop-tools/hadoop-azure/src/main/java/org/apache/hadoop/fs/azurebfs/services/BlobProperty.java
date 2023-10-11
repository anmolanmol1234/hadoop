/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

/**
 * Encapsulates all the information related to a Blob as fetched
 * on blob endpoint APIs
 */
public class BlobProperty {
    private String name;
    private Path path;
    private String url;
    private Boolean isDirectory = false;
    private String eTag;
    private long lastModifiedTime;
    private long creationTime;
    private String owner;
    private String group;
    private String permission;
    private String acl;
    private Long contentLength = 0L;
    private String copyId;
    private String copyStatus;
    private String copySourceUrl;
    private String copyProgress;
    private String copyStatusDescription;
    private long copyCompletionTime;
    private Map<String, String> metadata = new HashMap<>();

    private AzureBlobFileSystemException ex;

    public BlobProperty() {

    }

    public String getName() {
        return name;
    }

    public Path getPath() {
        return path;
    }

    public String getUrl() {
        return url;
    }

    public Boolean getIsDirectory() {
        return isDirectory;
    }

    public String getETag() {
        return eTag;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getOwner() {
        return owner;
    }

    public String getGroup() {
        return group;
    }

    public String getPermission() {
        return permission;
    }

    public String getAcl() {
        return acl;
    }

    public Long getContentLength() {
        return contentLength;
    }

    public String getCopyId() {
        return copyId;
    }

    public String getCopyStatus() {
        return copyStatus;
    }

    public String getCopySourceUrl() {
        return copySourceUrl;
    }

    public String getCopyProgress() {
        return copyProgress;
    }

    public String getCopyStatusDescription() {
        return copyStatusDescription;
    }

    public long getCopyCompletionTime() {
        return copyCompletionTime;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public AzureBlobFileSystemException getFailureException() {
        return ex;
    }

    public Path getBlobDstPath(Path dstBlobPath) {
        return null;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setPath(final Path path) {
        this.path = path;
    }

    public void setUrl(final String url) {
        this.url = url;
    }

    public void setIsDirectory(final Boolean isDirectory) {
        this.isDirectory = isDirectory;
    }

    public void setETag(final String eTag) {
        this.eTag = eTag;
    }

    public void setLastModifiedTime(final long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public void setCreationTime(final long creationTime) {
        this.creationTime = creationTime;
    }

    public void setOwner(final String owner) {
        this.owner = owner;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    public void setPermission(final String permission) {
        this.permission = permission;
    }

    public void setAcl(final String acl) {
        this.acl = acl;
    }

    public void setContentLength(final Long contentLength) {
        this.contentLength = contentLength;
    }

    public void setCopyId(final String copyId) {
        this.copyId = copyId;
    }

    public void setCopyStatus(final String copyStatus) {
        this.copyStatus = copyStatus;
    }

    public void setCopySourceUrl(final String copySourceUrl) {
        this.copySourceUrl = copySourceUrl;
    }

    public void setCopyProgress(final String copyProgress) {
        this.copyProgress = copyProgress;
    }

    public void setCopyStatusDescription(final String copyStatusDescription) {
        this.copyStatusDescription = copyStatusDescription;
    }

    public void setCopyCompletionTime(final long copyCompletionTime) {
        this.copyCompletionTime = copyCompletionTime;
    }

    public void addMetadata(String key, String value) {
        metadata.put(key, value);
    }
}
