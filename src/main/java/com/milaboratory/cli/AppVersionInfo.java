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

import java.util.HashMap;
import java.util.Objects;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.ANY,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
@Serializable(asJson = true)
public abstract class AppVersionInfo {
    /** get() function must be implemented in child class */
    protected static volatile AppVersionInfo instance = null;
    protected final HashMap<String, VersionInfo> componentVersions;
    protected final HashMap<String, String> componentStringVersions;

    protected AppVersionInfo(@JsonProperty("componentVersions") HashMap<String, VersionInfo> componentVersions,
                             @JsonProperty("componentStringVersions") HashMap<String, String> componentStringVersions) {
        this.componentVersions = componentVersions;
        this.componentStringVersions = componentStringVersions;
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

    public abstract String getShortestVersionString();

    public String getVersionString(OutputType outputType) {
        return getVersionString(outputType, false);
    }

    public abstract String getVersionString(OutputType outputType, boolean full);

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
