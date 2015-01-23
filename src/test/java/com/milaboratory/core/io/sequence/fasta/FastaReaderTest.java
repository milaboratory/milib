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
import com.milaboratory.core.io.sequence.SingleReadImpl;
import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.SequenceQuality;
import com.milaboratory.core.sequence.WildcardSymbol;
import org.apache.commons.math3.random.Well1024a;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class FastaReaderTest {

    @Test
    public void testRandomFile() throws Exception {
        int readsCount = 100;
        TestItem testItem = createTestData(System.currentTimeMillis(), readsCount, true);
        FastaReader reader = new FastaReader(testItem.file, true);
        for (int i = 0; i < readsCount; ++i) {
            SingleRead read = reader.take();
            Assert.assertEquals(testItem.reads[i].getId(), read.getId());
            Assert.assertEquals(testItem.reads[i].getDescription(), read.getDescription());
            Assert.assertEquals(testItem.reads[i].getData(), read.getData());
        }

        Assert.assertTrue(reader.take() == null);
        reader.close();
        testItem.file.delete();
    }

    @Test
    public void testRandomFileNoWildcards() throws Exception {
        int readsCount = 100;
        TestItem testItem = createTestData(System.currentTimeMillis(), readsCount, false);
        FastaReader reader = new FastaReader(testItem.file, false);
        for (int i = 0; i < readsCount; ++i) {
            SingleRead read = reader.take();
            Assert.assertEquals(testItem.reads[i].getId(), read.getId());
            Assert.assertEquals(testItem.reads[i].getDescription(), read.getDescription());
            Assert.assertEquals(testItem.reads[i].getData(), read.getData());
        }

        Assert.assertTrue(reader.take() == null);
        Assert.assertEquals(readsCount, reader.getNumberOfReads());
        reader.close();
        testItem.file.delete();
    }

    private static TestItem createTestData(long seed, int readsCount, boolean withWildcards) throws IOException {
        Well1024a rnd = new Well1024a(seed);

        File tempFile = File.createTempFile("temp" + seed, ".fasta");
        tempFile.deleteOnExit();
        FileOutputStream output = new FileOutputStream(tempFile);

        SingleRead[] reads = new SingleRead[readsCount];

        WildcardSymbol[] wildcards = NucleotideSequence.ALPHABET.getAllWildcards().toArray(new WildcardSymbol[0]);
        long id = 0;
        for (int i = 0; i < readsCount; ++i) {
            char[] seq = new char[50 + rnd.nextInt(100)];
            char[] seqExp = new char[seq.length];

            int qPointer = 0;
            byte[] qualityData = new byte[seq.length];
            Arrays.fill(qualityData, SequenceQuality.GOOD_QUALITY_VALUE);
            for (int j = 0; j < seq.length; ++j) {
                seq[j] = seqExp[j] = NucleotideSequence.ALPHABET.symbolFromCode((byte) rnd.nextInt(4));
                if (j != 0 && j != seq.length - 1 && ((4 * j) % seq.length == 0) && rnd.nextBoolean()) {
                    //next line for sequence
                    seq[j] = seqExp[j] = '\n';
                    --qPointer;
                } else if (withWildcards && j % 5 == 0) {//wildcard
                    WildcardSymbol wildcard = wildcards[rnd.nextInt(wildcards.length)];
                    if (NucleotideSequence.ALPHABET.codeFromSymbol(wildcard.getSymbol()) == -1) {
                        seq[j] = wildcard.getSymbol();
                        seqExp[j] = NucleotideSequence.ALPHABET.symbolFromCode(
                                wildcard.getUniformlyDistributedSymbol(
                                        id ^ (j + qPointer)));//as used in FastaReader#getSequenceWithQuality(..)
                        qualityData[j + qPointer] = SequenceQuality.BAD_QUALITY_VALUE;
                    }
                }
            }
            String description = ">seq" + i;
            String sequenceString = new String(seq);
            output.write(description.getBytes());
            output.write('\n');
            output.write(sequenceString.getBytes());
            output.write('\n');

            reads[i] = new SingleReadImpl(id,
                    new NSequenceWithQuality(new NucleotideSequence(
                            new String(seqExp).replace("\n", "")),
                            new SequenceQuality(Arrays.copyOfRange(qualityData, 0, seq.length + qPointer)))
                    , description.substring(1));
            ++id;
        }
        output.close();

        return new TestItem(tempFile, reads);
    }

    private static class TestItem {
        final File file;
        final SingleRead[] reads;

        private TestItem(File file, SingleRead[] reads) {
            this.file = file;
            this.reads = reads;
        }
    }
}