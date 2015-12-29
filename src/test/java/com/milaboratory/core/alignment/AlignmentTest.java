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
package com.milaboratory.core.alignment;


import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.primitivio.PrimitivI;
import com.milaboratory.primitivio.PrimitivO;
import com.milaboratory.util.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class AlignmentTest {

    public static NucleotideSequence mutate(NucleotideSequence seq, int[] mut) {
        return new Mutations<NucleotideSequence>(NucleotideSequence.ALPHABET, mut).mutate(seq);
    }

    static int[] move(int[] mutations, int offset) {
        return new Mutations<NucleotideSequence>(NucleotideSequence.ALPHABET, mutations).move(offset).getRAWMutations();
    }

    @Before
    public void setUp() throws Exception {
        //0123456L bad
        RandomUtil.getThreadLocalRandom().setSeed(014343433L);
    }

    @Test
    public void test1() throws Exception {
        NucleotideSequence sequence1 = new NucleotideSequence("TACCGCCAT");
        NucleotideSequence sequence2 = new NucleotideSequence("CCTCAT");
        Alignment<NucleotideSequence> alignment = Aligner.alignLocal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                sequence1, sequence2);
        Assert.assertEquals(3, alignment.convertPosition(6));
        Assert.assertEquals(0, alignment.convertPosition(2));
    }

    @Test
    public void test2() throws Exception {
        NucleotideSequence sequence1 = new NucleotideSequence("TACCGCCATGACCA");
        NucleotideSequence sequence2 = new NucleotideSequence("CCTCATCTCTT");
        Alignment<NucleotideSequence> alignment = Aligner.alignLocal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                sequence1, sequence2);

        AlignmentHelper helper = alignment.getAlignmentHelper();

        Assert.assertEquals(alignment.getSequence1Range().getFrom(), helper.getSequence1PositionAt(0));
        Assert.assertEquals(alignment.getSequence1Range().getTo() - 1,
                helper.getSequence1PositionAt(helper.size() - 1));

        Assert.assertEquals(alignment.getSequence2Range().getFrom(), helper.getSequence2PositionAt(0));
        Assert.assertEquals(alignment.getSequence2Range().getTo() - 1,
                helper.getSequence2PositionAt(helper.size() - 1));
    }

    @Test
    public void testInvert() throws Exception {
        NucleotideSequence seq0 = new NucleotideSequence("GATACATTAGACACAGATACA");
        NucleotideSequence seq1 = new NucleotideSequence("AGACACATATACACAG");
        NucleotideSequence seq2 = new NucleotideSequence("GATACGATACATTAGAGACCACAGATACA");

        Alignment<NucleotideSequence>[] alignments = new Alignment[]{
                Aligner.alignLocalAffine(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), seq0, seq1),
                Aligner.alignGlobalAffine(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), seq0, seq1),
                Aligner.alignLocalAffine(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), seq0, seq2),
                Aligner.alignGlobalAffine(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), seq0, seq2),
        };

        NucleotideSequence[] originalSeq = new NucleotideSequence[]{seq1, seq1, seq2, seq2};

        for (int i = 0; i < 4; i++) {
            System.out.println(alignments[i].getAlignmentHelper());
            System.out.println(alignments[i].invert(originalSeq[i]).getAlignmentHelper());
            Assert.assertEquals(
                    alignments[i].getAlignmentHelper().toString(),
                    alignments[i].invert(originalSeq[i]).invert(seq0).getAlignmentHelper().toString()
            );
            System.out.println();
        }
    }

    @Test
    public void testSerialization1() throws Exception {
        NucleotideSequence sequence1 = new NucleotideSequence("TACCGCCATGACCA");
        NucleotideSequence sequence2 = new NucleotideSequence("CCTCATCTCTT");
        Alignment<NucleotideSequence> alignment = Aligner.alignLocal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                sequence1, sequence2);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrimitivO po = new PrimitivO(bos);
        int cc = 10;
        for (int i = 0; i < cc; i++)
            po.writeObject(alignment);

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        PrimitivI pi = new PrimitivI(bis);

        for (int i = 0; i < cc; i++) {
            Alignment actual = pi.readObject(Alignment.class);
            Assert.assertEquals(alignment, actual);
        }
    }
}
