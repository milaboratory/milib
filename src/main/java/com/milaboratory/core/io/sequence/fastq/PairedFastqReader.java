/*
 * Copyright 2015 MiLaboratory.com
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
import com.milaboratory.core.io.sequence.PairedReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by dbolotin on 23/06/14.
 */
public final class PairedFastqReader extends PairedReader {
    public PairedFastqReader(File file1, File file2) throws IOException {
        this(new SingleFastqReader(file1),
                new SingleFastqReader(file2));
    }

    public PairedFastqReader(String fileName1, String fileName2) throws IOException {
        this(new SingleFastqReader(fileName1),
                new SingleFastqReader(fileName2));
    }

    public PairedFastqReader(File file1, File file2,
                             boolean lazyReads) throws IOException {
        this(new SingleFastqReader(file1, lazyReads),
                new SingleFastqReader(file2, lazyReads));
    }

    public PairedFastqReader(String fileName1, String fileName2,
                             boolean lazyReads) throws IOException {
        this(new SingleFastqReader(fileName1, lazyReads),
                new SingleFastqReader(fileName2, lazyReads));
    }


    public PairedFastqReader(File file1, File file2,
                             QualityFormat format, CompressionType ct) throws IOException {
        this(new SingleFastqReader(new FileInputStream(file1), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, true),
                new SingleFastqReader(new FileInputStream(file2), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, true));
    }

    public PairedFastqReader(String fileName1, String fileName2,
                             QualityFormat format, CompressionType ct) throws IOException {
        this(new SingleFastqReader(new FileInputStream(fileName1), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, true),
                new SingleFastqReader(new FileInputStream(fileName2), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, true));
    }

    public PairedFastqReader(File file1, File file2,
                             QualityFormat format, CompressionType ct,
                             boolean lazyReads) throws IOException {
        this(new SingleFastqReader(new FileInputStream(file1), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, lazyReads),
                new SingleFastqReader(new FileInputStream(file2), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, lazyReads));
    }

    public PairedFastqReader(String fileName1, String fileName2,
                             QualityFormat format, CompressionType ct,
                             boolean lazyReads) throws IOException {
        this(new SingleFastqReader(new FileInputStream(fileName1), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, lazyReads),
                new SingleFastqReader(new FileInputStream(fileName2), format, ct, false, SingleFastqReader.DEFAULT_BUFFER_SIZE, lazyReads));
    }

    public PairedFastqReader(InputStream stream1, InputStream stream2,
                             QualityFormat format, CompressionType ct,
                             boolean guessQualityFormat, int bufferSize, boolean lazyReads) throws IOException {
        this(new SingleFastqReader(stream1, format, ct, guessQualityFormat, bufferSize, lazyReads),
                new SingleFastqReader(stream2, format, ct, guessQualityFormat, bufferSize, lazyReads));
    }

    public PairedFastqReader(InputStream stream1, InputStream stream2) throws IOException {
        super(new SingleFastqReader(stream1), new SingleFastqReader(stream2));
    }

    public PairedFastqReader(InputStream stream1, InputStream stream2, CompressionType compressionType) throws IOException {
        super(new SingleFastqReader(stream1, compressionType), new SingleFastqReader(stream2, compressionType));
    }

    public PairedFastqReader(SingleFastqReader reader1, SingleFastqReader reader2) {
        super(reader1, reader2);
    }
}
