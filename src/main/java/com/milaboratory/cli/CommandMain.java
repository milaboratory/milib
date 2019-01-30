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

import picocli.CommandLine.*;

@Command(name = "main",
        versionProvider = CommandMain.VersionProvider.class,
        separator = " ")
public class CommandMain extends ABaseCommand {
    private static String[] versionInfo = null;

    public CommandMain(String appName) {
        super(appName);
    }

    public static void init(String[] versionInfoArg) {
        versionInfo = versionInfoArg;
    }

    @Option(names = {"-v", "--version"},
            versionHelp = true,
            description = "print version information and exit")
    boolean versionRequested;

    @Option(names = {"-h", "--help"},
            hidden = true)
    @Override
    public void requestHelp(boolean b) {
        throwValidationException("ERROR: -h / --help is not supported: use `" + appName + " help` for usage.");
    }

    public static final class VersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            if (versionInfo == null)
                throw new RuntimeException("getVersion() called while versionInfo is not initialized!");
            return versionInfo;
        }
    }
}
