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
package com.milaboratory.core.io.sequence.fasta;

import com.milaboratory.core.io.sequence.SingleRead;
import com.milaboratory.core.io.sequence.SingleSequenceWriter;

import java.io.*;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class FastaWriter implements SingleSequenceWriter {
    public static final int DEFAULT_MAX_LENGTH = 75;
    final int maxLength;
    final OutputStream outputStream;

    /**
     * Creates the writer
     *
     * @param fileName file to be created
     */
    public FastaWriter(String fileName) throws FileNotFoundException {
        this(new File(fileName), DEFAULT_MAX_LENGTH);
    }

    /**
     * Creates the writer
     *
     * @param file output file
     */
    public FastaWriter(File file, int maxLength) throws FileNotFoundException {
        this.outputStream = new BufferedOutputStream(new FileOutputStream(file));
        this.maxLength = maxLength;
    }

    public FastaWriter(OutputStream outputStream, int maxLength) {
        this.outputStream = outputStream;
        this.maxLength = maxLength;
    }

    @Override
    public synchronized void write(SingleRead read) {
        try {
            String description = read.getDescription();
            outputStream.write('>');
            if (description != null)
                outputStream.write(description.getBytes());
            outputStream.write('\n');

            byte[] sequence = read.getData().getSequence().toString().getBytes();
            int pointer = 0;
            while (true) {
                if (sequence.length - pointer <= maxLength) {
                    outputStream.write(sequence, pointer, sequence.length - pointer);
                    break;
                } else {
                    outputStream.write(sequence, pointer, maxLength);
                    pointer += maxLength;
                }
            }
            outputStream.write('\n');
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
