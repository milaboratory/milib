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

import picocli.CommandLine.Parameters;

import java.util.Collections;
import java.util.List;

public abstract class ACommandWithSmartOverwriteWithSingleInput extends ACommandWithSmartOverwrite {
    public ACommandWithSmartOverwriteWithSingleInput(String appName, BinaryFileInfoExtractor binaryFileInfoExtractor,
                                                     PipelineConfigurationReader pipelineConfigurationReader) {
        super(appName, binaryFileInfoExtractor, pipelineConfigurationReader);
    }

    public String in;
    public String out;

    @Parameters(index = "0", description = "input file")
    public void setIn(String in) {
        this.in = in;
    }

    @Parameters(index = "1", description = "output file")
    public void setOut(String out) {
        this.out = out;
    }

    @Override
    public final List<String> getOutputFiles() {
        return Collections.singletonList(out);
    }

    @Override
    public final List<String> getInputFiles() {
        return Collections.singletonList(in);
    }

    private boolean inputFileInfoInitialized = false;
    private BinaryFileInfo inputFileInfo = null;

    public BinaryFileInfo getInputFileInfo() {
        if (getInputFiles().size() != 1) throw new RuntimeException();
        if (!inputFileInfoInitialized) {
            inputFileInfo = binaryFileInfoExtractor.getFileInfo(in);
            inputFileInfoInitialized = true;
        }
        return inputFileInfo;
    }
}
