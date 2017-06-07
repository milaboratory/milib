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

public class VersionInfo {
    final String version, revision, name, branch, host;
    final Date timestamp;

    public VersionInfo(String version, String revision, String name, String branch, String host,
                       Date timestamp) {
        this.version = version;
        this.revision = revision;
        this.name = name;
        this.branch = branch;
        this.host = host;
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

    public String getHost() {
        return host;
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
                ", host='" + host + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static VersionInfo getVersionInfoForArtifact(String artifactId) {
        return getVersionInfo("/" + artifactId + "-build.properties");
    }

    static String longest(String s1, String s2) {
        if (s1 == null)
            return s2;
        else if (s2 == null)
            return s1;
        else if (s1.length() > s2.length())
            return s1;
        else
            return s2;
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
                properties.getProperty("host"),
                new Date(Long.parseLong(properties.getProperty("timestamp"))));
    }
}
