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

public abstract class PipelineConfigurationReader {
    private final BinaryFileInfoExtractor binaryFileInfoExtractor;

    public PipelineConfigurationReader(BinaryFileInfoExtractor binaryFileInfoExtractor) {
        this.binaryFileInfoExtractor = binaryFileInfoExtractor;
    }

    abstract PipelineConfiguration getPipelineConfiguration();

    /**
     * Read pipeline configuration from file or return null
     */
    PipelineConfiguration fromFileOrNull(String fileName, BinaryFileInfo fileInfo) {
        if (fileInfo == null)
            return null;
        if (!fileInfo.valid)
            return null;
        try {
            return fromFile(fileName, fileInfo);
        } catch (Throwable ignored) {}
        return null;
    }

    PipelineConfiguration fromFile(String fileName) {
        BinaryFileInfo fileInfo = binaryFileInfoExtractor.getFileInfo(fileName);
        if (!fileInfo.valid)
            throw new RuntimeException("File " + fileName + " corrupted.");
        return fromFile(fileName, fileInfo);
    }

    /**
     * Read pipeline configuration from file or throw exception
     */
    abstract PipelineConfiguration fromFile(String fileName, BinaryFileInfo fileInfo);
}
