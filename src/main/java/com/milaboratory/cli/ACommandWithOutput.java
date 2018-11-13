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

import picocli.CommandLine.Option;

import java.io.File;

/** A command which produce output files */
public abstract class ACommandWithOutput extends ACommand {
    public ACommandWithOutput(String appName) {
        super(appName);
    }

    @Option(names = {"-f", "--force-overwrite"},
            description = "Force overwrite of output file(s).")
    public boolean forceOverwrite = false;

    @Option(names = {"--force"}, hidden = true)
    public void setForce(boolean value) {
        if (value) {
            warn("--force option is deprecated; use --force-overwrite instead.");
            forceOverwrite = true;
        }
    }

    @Override
    public void validate() {
        super.validate();
        for (String f : getOutputFiles())
            if (new File(f).exists())
                handleExistenceOfOutputFile(f);
    }

    /** Specifies behaviour in the case with output exists (default is to throw exception) */
    public void handleExistenceOfOutputFile(String outFileName) {
        if (!forceOverwrite)
            throwValidationException("File \"" + outFileName
                    + "\" already exists. Use -f / --force-overwrite option to overwrite it.", false);
    }
}
