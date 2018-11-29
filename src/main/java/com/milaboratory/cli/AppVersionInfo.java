/*
 * Copyright 2018 MiLaboratory.com
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
package com.milaboratory.cli;

import com.fasterxml.jackson.annotation.*;
import com.milaboratory.primitivio.annotations.Serializable;
import com.milaboratory.util.VersionInfo;

import java.util.*;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.ANY,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@Serializable(asJson = true)
public final class AppVersionInfo {
    private static volatile AppVersionInfo instance = null;
    private final HashMap<String, VersionInfo> componentVersions;
    private final HashMap<String, String> componentStringVersions;

    private AppVersionInfo(@JsonProperty("componentVersions") HashMap<String, VersionInfo> componentVersions,
                           @JsonProperty("componentStringVersions") HashMap<String, String> componentStringVersions) {
        this.componentVersions = componentVersions;
        this.componentStringVersions = componentStringVersions;
    }

    public synchronized static void init(HashMap<String, VersionInfo> componentVersions,
                            HashMap<String, String> componentStringVersions) {
        if (instance == null)
            instance = new AppVersionInfo(componentVersions, componentStringVersions);
        else
            throw new IllegalStateException("Initialization of already initialized AppVersionInfo: "
                    + "componentVersions = " + componentVersions + ", componentStringVersions = "
                    + componentStringVersions + ", instance.componentVersions = " + instance.componentVersions
                    + ", instance.componentStringVersions = " + instance.componentStringVersions);
    }

    public synchronized static AppVersionInfo get() {
        if (instance == null)
            throw new IllegalStateException("AppVersionInfo not initialized!");
        else
            return instance;
    }

    public Map<String, VersionInfo> getComponentVersions() {
        return Collections.unmodifiableMap(componentVersions);
    }

    public Map<String, String> getComponentStringVersions() {
        return Collections.unmodifiableMap(componentStringVersions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppVersionInfo that = (AppVersionInfo) o;
        return Objects.equals(componentVersions, that.componentVersions) &&
                Objects.equals(componentStringVersions, that.componentStringVersions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentVersions, componentStringVersions);
    }

    public enum OutputType {
        ToConsole("\n", true), ToFile("; ", false);
        public final String delimiter;
        public final boolean componentsWord;

        OutputType(String delimiter, boolean componentsWord) {
            this.delimiter = delimiter;
            this.componentsWord = componentsWord;
        }
    }
}
