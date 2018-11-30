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

/** A command which allows resuming execution */
public abstract class ACommandWithSmartOverwrite extends ACommandWithOutput {
    protected final BinaryFileInfoExtractor binaryFileInfoExtractor;
    protected final PipelineConfigurationReader pipelineConfigurationReader;

    public ACommandWithSmartOverwrite(String appName, BinaryFileInfoExtractor binaryFileInfoExtractor,
                                      PipelineConfigurationReader pipelineConfigurationReader) {
        super(appName);
        this.binaryFileInfoExtractor = binaryFileInfoExtractor;
        this.pipelineConfigurationReader = pipelineConfigurationReader;
    }

    @Option(names = "--overwrite-if-required",
            description = "Overwrite output file if it is corrupted or if it was generated from different input file " +
                    "or with different parameters. -f / --force-overwrite overrides this option.")
    public boolean overwriteIfRequired = false;

    /** returns the unique run configuration */
    public abstract ActionConfiguration getConfiguration();

    /** returns the full pipeline configuration that will be written to the output file */
    public abstract PipelineConfiguration getFullPipelineConfiguration();

    public final String getOutput() {
        return getOutputFiles().get(0);
    }

    private boolean outputFileInfoInitialized = false;
    private BinaryFileInfo outputFileInfo = null;

    public BinaryFileInfo getOutputFileInfo() {
        if (getOutputFiles().size() != 1) throw new RuntimeException();
        if (!outputFileInfoInitialized) {
            outputFileInfo = binaryFileInfoExtractor.getFileInfo(getOutput());
            outputFileInfoInitialized = true;
        }
        return outputFileInfo;
    }

    @Override
    public void validate() {
        super.validate();
        if (getOutputFiles().size() != 1)
            throwValidationException("single output file expected");
    }

    /** whether to skip execution or not */
    private boolean skipExecution = false;

    @Override
    public void handleExistenceOfOutputFile(String outFileName) {
        if (forceOverwrite)
            // rewrite anyway
            return;

        // analysis supposed to be performed now
        PipelineConfiguration expectedPipeline = getFullPipelineConfiguration();
        // history written in existing file
        PipelineConfiguration actualPipeline = pipelineConfigurationReader.fromFileOrNull(outFileName,
                getOutputFileInfo());

        if ((actualPipeline != null) && actualPipeline.compatibleWith(expectedPipeline)) {
            String exists = "File " + outFileName + " already exists and contains correct " +
                    "binary data obtained from the specified input file. ";

            if (!overwriteIfRequired)
                throwValidationException(exists +
                        "Use --overwrite-if-required option to skip execution (output file will remain unchanged) " +
                        "or use -f / --force-overwrite option to force overwrite it.", false);
            else {
                warn("Skipping " + expectedPipeline.lastConfiguration().actionName() + ". " + exists);

                // print warns in case different app versions
                for (int i = 0; i < expectedPipeline.pipelineSteps.length; i++) {
                    ActionConfiguration
                            prev = actualPipeline.pipelineSteps[i].configuration,
                            curr = expectedPipeline.pipelineSteps[i].configuration;
                    if (!prev.versionId().equals(curr.versionId()))
                        warn(String.format("WARNING (--overwrite-if-required): %s was performed with previous " +
                                        appName + " version (%s). Consider re-running analysis using " +
                                        "--force-overwrite option.",
                                prev.actionName(),
                                actualPipeline.pipelineSteps[i].versionInfo));
                }

                skipExecution = true; // nothing to do in run0, just exit
                return;
            }
        }
        if (overwriteIfRequired)
            return;
        super.handleExistenceOfOutputFile(outFileName);
    }

    @Override
    public final void run0() throws Exception {
        if (skipExecution)
            return;
        run1();
    }

    public abstract void run1() throws Exception;
}
