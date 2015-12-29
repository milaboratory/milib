/*
 * Copyright 2015 MiLaboratory.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.milaboratory.util;

import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Created by dbolotin on 21/10/15.
 */
public class VersionInfo {
    final String version, revision, name, branch;
    final Date timestamp;

    public VersionInfo(String version, String revision, String name, String branch,
                       Date timestamp) {
        this.version = version;
        this.revision = revision;
        this.name = name;
        this.branch = branch;
        this.timestamp = timestamp;
    }

    public String getVersion() {
        return version;
    }

    public String getRevision() {
        return revision;
    }

    public String getName() {
        return name;
    }

    public String getBranch() {
        return branch;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "VersionInfo{" +
                "version='" + version + '\'' +
                ", revision='" + revision + '\'' +
                ", name='" + name + '\'' +
                ", branch='" + branch + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static VersionInfo getVersionInfoForArtifact(String artifactId) {
        return getVersionInfo("/" + artifactId + "-build.properties");
    }

    static VersionInfo getVersionInfo(String resourceName) {
        Properties properties = new Properties();
        try (InputStream is = VersionInfo.class.getResourceAsStream(resourceName)) {
            properties.load(is);
        } catch (Exception ex) {
            return null;
        }
        return new VersionInfo(properties.getProperty("version"),
                properties.getProperty("revision"),
                properties.getProperty("name"),
                properties.getProperty("branch"),
                new Date(Long.parseLong(properties.getProperty("timestamp"))));
    }
}
