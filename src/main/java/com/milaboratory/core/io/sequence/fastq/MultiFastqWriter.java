/*
 * Copyright 2017 MiLaboratory.com
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
package com.milaboratory.core.io.sequence.fastq;

import com.milaboratory.core.io.CompressionType;
import com.milaboratory.core.io.sequence.AbstractMultiWriter;
import com.milaboratory.core.io.sequence.MultiRead;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public final class MultiFastqWriter extends AbstractMultiWriter<MultiRead> {
    public MultiFastqWriter(File... files) throws IOException {
        this(filesToWriters(files));
    }

    public MultiFastqWriter(String... fileNames) throws IOException {
        this(stringsToWriters(fileNames));
    }

    public MultiFastqWriter(QualityFormat qualityFormat, CompressionType ct, String... fileNames) throws IOException {
        this(stringsWithParametersToWriters(qualityFormat, ct, fileNames));
    }

    public MultiFastqWriter(SingleFastqWriter... writers) {
        super(writers);
    }

    private static SingleFastqWriter[] filesToWriters(File... files) throws IOException {
        SingleFastqWriter[] writers = new SingleFastqWriter[files.length];
        for (int i = 0; i < files.length; i++)
            writers[i] = new SingleFastqWriter(files[i]);
        return writers;
    }

    private static SingleFastqWriter[] stringsToWriters(String... fileNames) throws IOException {
        SingleFastqWriter[] writers = new SingleFastqWriter[fileNames.length];
        for (int i = 0; i < fileNames.length; i++)
            writers[i] = new SingleFastqWriter(fileNames[i]);
        return writers;
    }

    private static SingleFastqWriter[] stringsWithParametersToWriters(QualityFormat qualityFormat, CompressionType ct,
                                                                      String... fileNames) throws IOException {
        SingleFastqWriter[] writers = new SingleFastqWriter[fileNames.length];
        for (int i = 0; i < fileNames.length; i++)
            writers[i] = new SingleFastqWriter(new FileOutputStream(fileNames[i]), qualityFormat, ct,
                    SingleFastqWriter.DEFAULT_BUFFER_SIZE);
        return writers;
    }
}
