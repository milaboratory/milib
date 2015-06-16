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

import com.milaboratory.core.io.sequence.IllegalFileFormatException;
import com.milaboratory.core.io.sequence.SingleRead;
import com.milaboratory.core.io.sequence.SingleReadImpl;
import com.milaboratory.core.io.sequence.SingleReader;
import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.SequenceQuality;
import com.milaboratory.core.sequence.WildcardSymbol;
import com.milaboratory.util.Bit2Array;
import com.milaboratory.util.CanReportProgress;
import com.milaboratory.util.CountingInputStream;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.milaboratory.core.sequence.NucleotideSequence.ALPHABET;

/**
 * FASTA reader for nucleotide sequences.
 *
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class FastaReader implements SingleReader, CanReportProgress {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    //lets read line by line
    private BufferedReader reader;
    private final boolean withWildcards;
    private String bufferedLine;
    private long id;
    private final long size;
    private final CountingInputStream countingInputStream;

    /**
     * Creates reader from the specified input stream.
     *
     * @param inputStream   input stream
     * @param withWildcards if {@code true}, then for each wildcard a
     *                      uniformly distributed nucleotide will be generated from the set of nucleotides
     *                      corresponding to this wildcard.
     * @param size          file size
     */
    public FastaReader(InputStream inputStream, boolean withWildcards, long size) {
        this.size = size;
        this.countingInputStream = new CountingInputStream(inputStream);
        this.reader = new BufferedReader(new InputStreamReader(countingInputStream));
        this.withWildcards = withWildcards;
    }

    /**
     * Creates reader from the specified input stream.
     *
     * @param inputStream   input stream
     * @param withWildcards if {@code true}, then for each wildcard a
     *                      uniformly distributed nucleotide will be generated from the set of nucleotides
     *                      corresponding to this wildcard.
     */
    public FastaReader(InputStream inputStream, boolean withWildcards) {
        this(inputStream, withWildcards, 0);
    }

    /**
     * Creates reader from the specified file.
     *
     * @param file          file
     * @param withWildcards if {@code true}, then for each wildcard a
     *                      uniformly distributed nucleotide will be generated from the set of nucleotides
     *                      corresponding to this wildcard.
     */
    public FastaReader(String file, boolean withWildcards)
            throws FileNotFoundException {
        this(new File(file), withWildcards);
    }

    /**
     * Creates reader from the specified file.
     *
     * @param file          file
     * @param withWildcards if {@code true}, then for each wildcard a
     *                      uniformly distributed nucleotide will be generated from the set of nucleotides
     *                      corresponding to this wildcard.
     */
    public FastaReader(File file, boolean withWildcards) throws FileNotFoundException {
        this(new FileInputStream(file), withWildcards, file.length());
    }

    @Override
    public synchronized double getProgress() {
        if (size == 0)
            return Double.NaN;
        return countingInputStream.getBytesRead() * 1.0 / size;
    }

    private volatile boolean isFinished = false;

    @Override
    public synchronized boolean isFinished() {
        return isFinished;
    }

    @Override
    public synchronized SingleRead take() {
        Item item;
        try {
            item = nextItem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (item == null) {
            isFinished = true;
            return null;
        }

        SingleRead read = new SingleReadImpl(id, getSequenceWithQuality(item.sequence), item.description);
        ++id; // don't increment before this point, because id is used in #getSequenceWithQuality(...)
        return read;
    }

    @Override
    public synchronized long getNumberOfReads() {
        return id;
    }

    private NSequenceWithQuality getSequenceWithQuality(String sequence) {
        byte[] qualityData = new byte[sequence.length()];
        Arrays.fill(qualityData, SequenceQuality.GOOD_QUALITY_VALUE);

        Bit2Array sequenceData = new Bit2Array(sequence.length());
        byte nucleotide;
        char symbol;
        for (int i = 0; i < sequence.length(); ++i) {
            symbol = sequence.charAt(i);
            nucleotide = ALPHABET.codeFromSymbol(symbol);
            if (nucleotide == -1) //wildChard
            {
                if (withWildcards) {
                    WildcardSymbol wildcard = ALPHABET.getWildcardFor(symbol);
                    if (wildcard == null)
                        throw new IllegalFileFormatException("Unknown wildcard: " + symbol + ".");
                    nucleotide = wildcard.getUniformlyDistributedSymbol(id ^ i);
                } else
                    throw new RuntimeException("Unknown letter: " + symbol);
                qualityData[i] = SequenceQuality.BAD_QUALITY_VALUE;
            }
            sequenceData.set(i, nucleotide);
        }

        return new NSequenceWithQuality(new NucleotideSequence(sequenceData),
                new SequenceQuality(qualityData));
    }

    private Item nextItem() throws IOException {
        String description;
        if (bufferedLine != null)
            description = bufferedLine;
        else {
            description = reader.readLine();
            if (description == null)
                return null;
            if (description.charAt(0) != '>')
                throw new IllegalFileFormatException("Wrong FASTA format.");
        }
        StringBuilder sequence = new StringBuilder();
        String line;
        while (true) {
            line = reader.readLine();
            if (line == null)
                break;
            if (!line.isEmpty() && line.charAt(0) == '>')
                break;
            sequence.append(line);
        }
        bufferedLine = line;
        return new Item(description.substring(1), sequence.toString());
    }

    /**
     * Closes the reader
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true))
            return;

        //is synchronized with itself and _next calls,
        //so no synchronization on innerReader is needed
        try {
            synchronized (reader) {
                reader.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final class Item {
        final String description;
        final String sequence;

        private Item(String description, String sequence) {
            this.description = description;
            this.sequence = sequence;
        }
    }
}
