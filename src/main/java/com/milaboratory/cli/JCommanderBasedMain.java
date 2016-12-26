/*
 * Copyright 2016 MiLaboratory.com
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class JCommanderBasedMain implements ActionHelper {
    // LinkedHashMap to preserve order of actions
    protected final Map<String, Action> actions = new LinkedHashMap<>();
    protected final String command;
    protected boolean printHelpOnError = false;
    protected boolean printStackTrace = false;
    protected Runnable versionInfoCallback = null;
    protected PrintStream outputStream = System.err;
    protected String[] arguments;

    public JCommanderBasedMain(String command, Action... actions) {
        this.command = command;
        for (Action action : actions)
            reg(action);
    }

    public void setOutputStream(PrintStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public PrintStream getDefaultPrintStream() {
        return outputStream;
    }

    @Override
    public String getCommandLineArguments() {
        StringBuilder builder = new StringBuilder();
        for (String arg : arguments) {
            if (builder.length() != 0)
                builder.append(" ");
            builder.append(arg);
        }
        return builder.toString();
    }

    public boolean isPrintStackTrace() {
        return printStackTrace;
    }

    public void setPrintStackTrace(boolean printStackTrace) {
        this.printStackTrace = printStackTrace;
    }

    protected void reg(Action a) {
        actions.put(a.command(), a);
    }

    public ProcessResult main(String... args) throws Exception {
        // Saving current arguments
        this.arguments = args;

        if (args.length == 0) {
            printGlobalHelp();
            return ProcessResult.Help;
        }

        // Setting up JCommander
        MainParameters mainParameters = getMainParameters();
        JCommander commander = new JCommander(mainParameters);
        commander.setProgramName(command);
        for (Action a : actions.values())
            commander.addCommand(a.command(), a.params());

        // Getting command name
        String commandName = args[0];

        // Getting corresponding action
        Action action = actions.get(commandName);

        try {
            if (action != null && (action instanceof ActionParametersParser)) {
                ((ActionParametersParser) action).parseParameters(Arrays.copyOfRange(args, 1, args.length));
            } else {
                commander.parse(args);

                // Print Version information if requested and exit.
                if (mainParameters instanceof MainParametersWithVersion &&
                        ((MainParametersWithVersion) mainParameters).version()) {
                    versionInfoCallback.run();
                    return ProcessResult.Version;
                }

                // Print complete help if requested
                if (mainParameters.help()) {
                    // Creating new instance of jCommander to add only non-hidden actions
                    printGlobalHelp();
                    return ProcessResult.Help;
                }

                if (args.length == 1 && !args[0].startsWith("-")) {
                    action = actions.get(commandName);
                    if (!action.getClass().isAnnotationPresent(AllowNoArguments.class)) {
                        System.out.println("Error: missing required arguments.\n");
                        printActionHelp(commander, action);
                        return ProcessResult.Error;
                    }
                }

                // Getting parsed command
                // assert parsedCommand.equals(commandName)
                final String parsedCommand = commander.getParsedCommand();

                // Processing potential errors
                if (parsedCommand == null || !actions.containsKey(parsedCommand)) {
                    if (parsedCommand == null)
                        outputStream.println("No command specified.");
                    else
                        outputStream.println("Command " + parsedCommand + " not supported.");
                    outputStream.println("Use -h option to get a list of supported commands.");
                    return ProcessResult.Error;
                }

                action = actions.get(parsedCommand);
            }

            if (action.params().help()) {
                printActionHelp(commander, action);
            } else {
                action.params().validate();
                action.go(this);
            }
        } catch (ParameterException | ProcessException pe) {
            printException(pe, commander, action);
            return ProcessResult.Error;
        }
        return ProcessResult.Ok;
    }

    private MainParameters getMainParameters() {
        return versionInfoCallback != null ?
                new MainParametersWithVersion() :
                new MainParameters();
    }

    protected void printGlobalHelp() {
        // Creating new instance of jCommander to add only non-hidden actions
        JCommander tmpCommander = new JCommander(getMainParameters());
        tmpCommander.setProgramName(command);
        for (Action a : actions.values())
            if (!a.getClass().isAnnotationPresent(HiddenAction.class))
                tmpCommander.addCommand(a.command(), a.params());
        StringBuilder builder = new StringBuilder();
        tmpCommander.usage(builder);
        outputStream.print(builder);
    }

    protected void printActionHelp(JCommander commander, Action action) {
        StringBuilder builder = new StringBuilder();
        if (action instanceof ActionHelpProvider) {
            if (((ActionHelpProvider) action).printDefaultHelp()) {
                commander.usage(action.command(), builder);
                builder.append("\n");
            }
            ((ActionHelpProvider) action).printHelp(builder);
        } else
            commander.usage(action.command(), builder);
        outputStream.print(builder);
    }

    protected void printException(RuntimeException e,
                                  JCommander commander, Action action) {
        outputStream.println("Error: " + e.getMessage());
        if (printStackTrace)
            e.printStackTrace(new PrintStream(outputStream));
        if (printHelpOnError)
            printActionHelp(commander, action);
    }

    public enum ProcessResult {
        Ok, Version, Help, Error
    }

    /**
     * Enables -v / --version parameter.
     *
     * Sets callback that will be invoked if this option is specified by user.
     *
     * {@literal null} disables -v parameter.
     *
     * @param versionInfoCallback callback to be will be invoked if user specified -v option. {@literal null} disables
     *                            -v parameter.
     */
    public void setVersionInfoCallback(Runnable versionInfoCallback) {
        this.versionInfoCallback = versionInfoCallback;
    }

    public static class MainParameters {
        @Parameter(names = {"-h", "--help"}, help = true, description = "Displays this help message.")
        public Boolean help;

        public boolean help() {
            return help != null && help;
        }
    }

    public static class MainParametersWithVersion extends MainParameters {
        @Parameter(names = {"-v", "--version"}, help = true, description = "Output version information.")
        public Boolean version;

        public boolean version() {
            return version != null && version;
        }
    }
}
