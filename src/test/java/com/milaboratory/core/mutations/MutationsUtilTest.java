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
package com.milaboratory.core.mutations;

import com.milaboratory.core.sequence.*;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well44497a;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class MutationsUtilTest {
    @Test
    public void test1() throws Exception {
        String m = "SA2TDC3";
        Assert.assertEquals(m,
                MutationsUtil.encode(MutationsUtil.decode(m, NucleotideSequence.ALPHABET), NucleotideSequence.ALPHABET));

        m = "SA2TD*3I12A";
        Assert.assertEquals(m,
                MutationsUtil.encode(MutationsUtil.decode(m, AminoAcidSequence.ALPHABET), AminoAcidSequence.ALPHABET));

        m = "SA2TD*3I12.";
        Assert.assertEquals(m,
                MutationsUtil.encode(MutationsUtil.decode(m, IncompleteAminoAcidSequence.ALPHABET),
                        IncompleteAminoAcidSequence.ALPHABET));
    }

    @Test
    public void test2() throws Exception {
        NSequenceWithQuality seq = new NSequenceWithQuality(
                "ATTAGACA",
                "ACBBAACC");
        Mutations<NucleotideSequence> mutations = Mutations.decode("DA0 I6C SA7G", NucleotideSequence.ALPHABET);
        NSequenceWithQuality expected = new NSequenceWithQuality(
                "TTAGACCG",
                "CBBAABCC");
        Assert.assertEquals(expected, MutationsUtil.mutate(seq, mutations));
    }

    @Test
    public void testRandomEncodeDecode() throws Exception {
        RandomGenerator rnd = new Well44497a();
        int n = 1000;
        for (Alphabet alphabet : Alphabets.getAll()) {
            for (int i = 0; i < n; ++i) {
                int[] r = createRandomMutations(alphabet, rnd.nextInt(100), rnd);
                Assert.assertArrayEquals(r, MutationsUtil.decode(MutationsUtil.encode(r, alphabet), alphabet));
            }
        }
    }

    @Test
    public void test1111() throws Exception {
        System.out.println(MutationsUtil.getMutationPatternStringForAlphabet(NucleotideSequence.ALPHABET));
        System.out.println(MutationsUtil.getMutationPatternStringForAlphabet(AminoAcidSequence.ALPHABET));
    }

    private static int[] createRandomMutations(Alphabet alphabet, int size, RandomGenerator rnd) {
        int[] r = new int[size];
        int pos = 0;
        byte type;
        for (int i = 0; i < size; ++i) {
            type = (byte) rnd.nextInt(3);
            switch (type) {
                case 0:
                    r[i] = Mutation.createSubstitution(pos, rnd.nextInt(alphabet.size()), rnd.nextInt(alphabet.size()));
                    break;
                case 1:
                    r[i] = Mutation.createDeletion(pos, rnd.nextInt(alphabet.size()));
                    break;
                case 2:
                    r[i] = Mutation.createInsertion(pos, rnd.nextInt(alphabet.size()));
                    break;
            }

            if (type != 2)
                ++pos;
            pos += rnd.nextInt(10);
        }
        return r;
    }
}