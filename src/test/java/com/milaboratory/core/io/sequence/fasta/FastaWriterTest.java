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
import com.milaboratory.util.Bit2Array;
import org.apache.commons.math3.random.Well1024a;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class FastaWriterTest {
    @Test
    public void test1() throws Exception {
        int count = 100;
        SingleRead[] reads = new SingleRead[count];
        File temp = File.createTempFile("temp", ".fasta");
        temp.deleteOnExit();
        FastaWriter writer = new FastaWriter(temp, 50);
        for (int i = 0; i < count; ++i) {
            reads[i] = randomRead(i);
            writer.write(reads[i]);
        }
        writer.close();
        FastaReader reader = new FastaReader(temp, false);
        for (int i = 0; i < count; ++i) {
            SingleRead actual = reader.take();
            Assert.assertEquals(reads[i].getDescription(), actual.getDescription());
            Assert.assertEquals(reads[i].getData(), actual.getData());
        }
        Assert.assertTrue(reader.take() == null);
        reader.close();
        temp.delete();
    }

    private static SingleRead randomRead(long id) {
        Well1024a random = new Well1024a(id);
        Bit2Array seq = new Bit2Array(50 + random.nextInt(150));
        for (int i = 0; i < seq.size(); ++i)
            seq.set(i, (byte) random.nextInt(NucleotideSequence.ALPHABET.size()));
        byte[] quality = new byte[seq.size()];
        Arrays.fill(quality, SequenceQuality.GOOD_QUALITY_VALUE);
        return new SingleReadImpl(id,
                new NSequenceWithQuality(new NucleotideSequence(seq), new SequenceQuality(quality)), "id" + id);
    }
}