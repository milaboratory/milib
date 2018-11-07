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

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.util.stream.Collectors;

public class ABaseCommand {
    protected final String appName;

    public ABaseCommand(String appName) {
        this.appName = appName;
    }

    @Spec
    public CommandSpec spec; // injected by picocli

    @Option(names = {"-h", "--help"},
            hidden = true)
    public void requestHelp(boolean b) {
        throwValidationException("ERROR: -h / --help is not supported: use `" + appName
                + " help [command]` for command usage.");
    }

    /** Throws validation exception */
    public void throwValidationException(String message, boolean printHelp) {
        throw new ValidationException(spec.commandLine(), message, printHelp);
    }

    /** Throws validation exception */
    public void throwValidationException(String message) {
        throwValidationException(message, true);
    }

    /** Throws execution exception */
    public void throwExecutionException(String message) {
        throw new CommandLine.ExecutionException(spec.commandLine(), message);
    }

    public String getCommandLineArguments() {
        return spec.commandLine().getParseResult().originalArgs().stream().collect(Collectors.joining(" "));
    }
}
