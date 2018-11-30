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

public abstract class ACommandSimpleExport extends ACommandWithOutput {
    public ACommandSimpleExport(String appName) {
        super(appName);
    }

    @Parameters(index = "0", description = "input_file")
    public String in;

    @Parameters(index = "1", description = "[output_file]", arity = "0..1")
    public String out = null;

    @Override
    public List<String> getInputFiles() {
        return Collections.singletonList(in);
    }

    @Override
    protected List<String> getOutputFiles() {
        return out == null ? Collections.emptyList() : Collections.singletonList(out);
    }
}
