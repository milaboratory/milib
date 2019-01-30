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
import java.util.*;

public abstract class ACommand extends ABaseCommand implements Runnable {
    public ACommand(String appName) {
        super(appName);
    }

    /** queue of warning messages */
    private List<String> warningsQueue = new ArrayList<>();
    /** flag that signals we are entered the run method */
    private boolean running;

    @Option(names = {"-nw", "--no-warnings"},
            description = "Suppress all warning messages.")
    public boolean quiet = false;

    @Option(description = "Verbose warning messages.",
            names = {"--verbose"})
    public boolean verbose = false;

    /** Warning message */
    public void warn(String message) {
        if (quiet)
            return;

        if (!running)
            // add to a queue
            warningsQueue.add(message);
        else
            // print immediately
            printWarn(message);
    }

    private void printWarn(String message) {
        if (!quiet)
            System.err.println(message);
    }

    /** list of input files */
    protected List<String> getInputFiles() {
        return Collections.emptyList();
    }

    /** list of output files produces as result */
    protected List<String> getOutputFiles() {
        return Collections.emptyList();
    }

    /** Validate injected parameters and options */
    public void validate() {
        for (String in : getInputFiles()) {
            if (!new File(in).exists())
                throwValidationException("ERROR: input file \"" + in + "\" does not exist.", false);
            validateInfo(in);
        }
    }

    /** Validate control information that file is not corrupted */
    public abstract void validateInfo(String inputFile);

    @Override
    public final void run() {
        validate();
        if (!quiet)
            for (String m : warningsQueue)
                printWarn(m);

        running = true;
        try {
            run0();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Do actual job */
    public abstract void run0() throws Exception;
}
