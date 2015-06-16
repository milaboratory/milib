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

import com.milaboratory.core.alignment.Aligner;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.alignment.LinearGapAlignmentScoring;
import com.milaboratory.core.io.util.TestUtil;
import com.milaboratory.core.sequence.NucleotideSequence;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class MutationsTest {

    @Test
    public void testCombine1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("ATTAGACA"),
                seq2 = new NucleotideSequence("CATTACCA"),
                seq3 = new NucleotideSequence("CATAGCCA");

        Mutations<NucleotideSequence> m1 = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), seq1, seq2).getAbsoluteMutations(),
                m2 = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), seq2, seq3).getAbsoluteMutations();

        checkMutations(m1);
        checkMutations(m2);
        Mutations<NucleotideSequence> m3 = m1.combineWith(m2);
        assertTrue(MutationsUtil.check(m3));
        Assert.assertEquals(seq3, m3.mutate(seq1));
    }

    @Test
    public void testCombine2() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("ACGTGTTACCGGTGATT"),
                seq2 = new NucleotideSequence("AGTTCTTGTTTTTTCCGTAC"),
                seq3 = new NucleotideSequence("ATCCGTAAATTACGTGCTGT");

        Mutations<NucleotideSequence> m1 = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), seq1, seq2).getAbsoluteMutations(),
                m2 = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), seq2, seq3).getAbsoluteMutations();

        Mutations<NucleotideSequence> m3 = m1.combineWith(m2);

        assertTrue(MutationsUtil.check(m3));
        checkMutations(m1);
        checkMutations(m2);
        checkMutations(m3);

        Assert.assertEquals(seq3, m3.mutate(seq1));
    }

    @Test
    public void testCombine3() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("AACTGCTAACTCGA"),
                seq2 = new NucleotideSequence("CGAACGTTAAGCACAAA"),
                seq3 = new NucleotideSequence("CAAATGTGAGATC");

        Mutations<NucleotideSequence> m1 = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), seq1, seq2).getAbsoluteMutations(),
                m2 = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), seq2, seq3).getAbsoluteMutations();

        Mutations<NucleotideSequence> m3 = m1.combineWith(m2);

        assertTrue(MutationsUtil.check(m3));
        checkMutations(m1);
        checkMutations(m2);
        checkMutations(m3);

        Assert.assertEquals(seq3, m3.mutate(seq1));
    }

    @Test
    public void testBS() throws Exception {
        NucleotideSequence
                seq1 = new NucleotideSequence("TGACCCGTAACCCCCCGGT"),
                seq2 = new NucleotideSequence("CGTAACTTCAGCCT");

        Alignment<NucleotideSequence> alignment = Aligner.alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                seq1, seq2);

//        AlignmentHelper helper = alignment.getAlignmentHelper();
//        System.out.println(helper);
//
//        int p;
//        for (int i = helper.size() - 1; i >= 0; --i) {
//            if ((p = helper.getSequence1PositionAt(i)) > 0)
//                assertEquals(NucleotideSequence.ALPHABET.symbolFromCode(seq1.codeAt(p)),
//                        helper.getLine1().charAt(i));
//            if ((p = helper.getSequence2PositionAt(i)) > 0)
//                assertEquals(NucleotideSequence.ALPHABET.symbolFromCode(seq2.codeAt(p)),
//                        helper.getLine3().charAt(i));
//        }

        Mutations<NucleotideSequence> mutations = alignment.getAbsoluteMutations();
        checkMutations(mutations);

        Assert.assertEquals(-1, mutations.firstMutationWithPosition(-1));
        Assert.assertEquals(3, mutations.firstMutationWithPosition(3));
        Assert.assertEquals(4, mutations.firstMutationWithPosition(4));
        Assert.assertEquals(-6, mutations.firstMutationWithPosition(5));
        Assert.assertEquals(5, mutations.firstMutationWithPosition(11));
        Assert.assertEquals(7, mutations.firstMutationWithPosition(12));
        Assert.assertEquals(8, mutations.firstMutationWithPosition(13));
    }

    public static void checkMutations(Mutations mutations) {
        assertEquals("Encode/Decode", mutations, Mutations.decode(mutations.encode(), NucleotideSequence.ALPHABET));
    }

    @Test
    public void test1() throws Exception {
        MutationsBuilder<NucleotideSequence> builder = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        builder.appendDeletion(1, 2);
        builder.appendDeletion(2, 1);
        builder.appendSubstitution(7, 3, 1);
        builder.appendInsertion(9, 2);
        builder.appendSubstitution(10, 3, 1);
        Mutations<NucleotideSequence> mutations = builder.createAndDestroy();
        assertEquals(1, mutations.getPositionByIndex(0));
        assertEquals(1, mutations.getFromAsCodeByIndex(1));
        assertEquals(3, mutations.getFromAsCodeByIndex(2));
        assertEquals(1, mutations.getToAsCodeByIndex(2));
        assertEquals(9, mutations.getPositionByIndex(3));
        assertEquals(2, mutations.getToAsCodeByIndex(3));
        assertEquals(1, mutations.getToAsCodeByIndex(4));
        assertEquals(10, mutations.getPositionByIndex(4));
    }

    @Test
    public void test2() throws Exception {
        MutationsBuilder<NucleotideSequence> builder = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        builder.appendDeletion(1, 2);
        builder.appendDeletion(2, 1);
        builder.appendSubstitution(7, 3, 1);
        builder.appendInsertion(9, 2);
        builder.appendSubstitution(10, 3, 1);
        Mutations<NucleotideSequence> mutations = builder.createAndDestroy();
        assertEquals(1, mutations.firsMutationPosition());
        assertEquals(10, mutations.lastMutationPosition());
    }

    @Test
    public void test3() throws Exception {
        MutationsBuilder<NucleotideSequence> builder = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        builder.appendDeletion(1, 2);
        builder.appendDeletion(2, 1);
        builder.appendSubstitution(7, 3, 1);
        builder.appendInsertion(9, 2);
        builder.appendSubstitution(10, 3, 1);
        Mutations<NucleotideSequence> se = builder.createAndDestroy();
        TestUtil.assertJavaSerialization(se);
    }
}