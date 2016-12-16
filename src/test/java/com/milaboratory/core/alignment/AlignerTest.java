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

import com.milaboratory.core.io.util.IOTestUtil;
import com.milaboratory.core.mutations.Mutation;
import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.mutations.MutationsUtil;
import com.milaboratory.core.mutations.generator.GenericNucleotideMutationModel;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.SubstitutionModels;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.AminoAcidSequence;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.test.TestUtil;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static com.milaboratory.core.alignment.Aligner.alignGlobal;
import static com.milaboratory.test.TestUtil.its;
import static com.milaboratory.test.TestUtil.randomSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignerTest {
    @Test
    public void testExtractSubstitutions1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("ATTAGACA"),
                seq2 = new NucleotideSequence("ACAGATAC");

        final Alignment muts = Aligner.alignOnlySubstitutions(seq1, seq2);

        assertEquals(seq2, muts.getAbsoluteMutations().mutate(seq1));
    }

    @Test
    public void testExtractSubstitutions2() throws Exception {
        AminoAcidSequence seq1 = new AminoAcidSequence("CASSLAPGAT"),
                seq2 = new AminoAcidSequence("CASGLASGLT");

        final Alignment muts = Aligner.alignOnlySubstitutions(seq1, seq2);

        assertEquals(seq2, muts.getAbsoluteMutations().mutate(seq1));
    }

    @Test
    public void testExtractSubstitutions3() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("ATTAGACA"),
                seq2 = new NucleotideSequence("ACAGATAC");

        final Alignment muts = Aligner.alignOnlySubstitutions(seq1, seq2, 0, seq1.size(), 0, seq2.size(),
                new AffineGapAlignmentScoring<>(NucleotideSequence.ALPHABET, 2, -1, -4, -3));

        assertEquals(seq2, muts.getAbsoluteMutations().mutate(seq1));
    }

    @Test
    public void testGlobal0() throws Exception {
        Alignment<NucleotideSequence> alignment = Aligner.alignGlobalLinear(
                LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                new NucleotideSequence("ATTAGACA"),
                new NucleotideSequence("ATTAAGA"));

        Assert.assertEquals(21.0, alignment.getScore(), 0.001);
    }

    @Test
    public void testGlobal1Linear() throws Exception {
        testGlobal1(LinearGapAlignmentScoring.getNucleotideBLASTScoring());
    }

    @Test
    public void testGlobal1Affine() throws Exception {
        testGlobal1(AffineGapAlignmentScoring.getNucleotideBLASTScoring());
    }

    public void testGlobal1(AlignmentScoring<NucleotideSequence> sc) throws Exception {
        NucleotideSequence seq2 = new NucleotideSequence("GGGAGGCATTAGACCAAT"),
                seq1 = new NucleotideSequence("ATTTTATACA");

        Alignment<NucleotideSequence> a = alignGlobal(sc,
                seq1, seq2);

        assertEquals(seq2, a.getAbsoluteMutations().mutate(seq1));
    }

    @Test
    public void testGlobalAA1() throws Exception {
        AminoAcidSequence seq1 = new AminoAcidSequence("ASSLPDRGQETQY"),
                seq2 = new AminoAcidSequence("ASSSPRGQETQRY");

        Alignment a = alignGlobal(LinearGapAlignmentScoring.getAminoAcidBLASTScoring(BLASTMatrix.BLOSUM62), seq1, seq2);
        assertEquals(seq2, a.getAbsoluteMutations().mutate(seq1));
    }

    @Test
    public void testGlobal2() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("TAGTTCCTTCTAT"),
                seq2 = new NucleotideSequence("GTTTA");

        Alignment a = alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                seq1, seq2);
        assertEquals(seq2, a.getAbsoluteMutations().mutate(seq1));
    }

    @Test
    public void testGlobalMove1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("TAGAATTAGACA"),
                seq2 = new NucleotideSequence("ATTAGTACA");
        NucleotideSequence s = new NucleotideSequence("GACATAC");
        Alignment<NucleotideSequence> a = alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                seq1, seq2);
        Mutations<NucleotideSequence> muts = a.getAbsoluteMutations();
        assertEquals(seq2.concatenate(s), muts.mutate(seq1.concatenate(s)));
        assertEquals(s.concatenate(seq2), muts.move(s.size()).mutate(s.concatenate(seq1)));
    }

    @Test
    public void testGlobalPosition1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("TAGAATTAGACA"),
                seq2 = new NucleotideSequence("ATTAGTAA");

        Alignment<NucleotideSequence> a = alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                seq1, seq2);
        Mutations<NucleotideSequence> muts = a.getAbsoluteMutations();

        int[] v = {-1, -1, -1, -1, 0, 1, 2, 3, 4, 6, -8, 7};
        for (int i = 0; i < seq1.size(); ++i)
            assertEquals(v[i], muts.convertToSeq2Position(i));
    }

    @Test
    public void testGlobalExtractCase1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("GAAGGAAAGCCCCAATTG"),
                seq2 = new NucleotideSequence("CGTGGAGATTATGTTAGA");
        Alignment<NucleotideSequence> a = alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                seq1, seq2);
        Mutations<NucleotideSequence> muts = a.getAbsoluteMutations();

        assertEquals(1, muts.extractMutationsForRange(17, 18).size());
    }

    @Test
    public void testGlobalExtract1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("TAGAAATAGACA"),
                seq2 = new NucleotideSequence("ATTAGTACA");

        Alignment<NucleotideSequence> a = alignGlobal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                seq1, seq2);
        Mutations<NucleotideSequence> muts = a.getAbsoluteMutations();

        assertEquals(seq2.getRange(
                sAbs(muts.convertToSeq2Position(2)),
                sAbs(muts.convertToSeq2Position(10))),
                muts.extractMutationsForRange(2, 10).mutate(seq1.getRange(2, 10)));

        assertEquals(seq2.getRange(
                sAbs(muts.convertToSeq2Position(2)),
                sAbs(muts.convertToSeq2Position(11))),
                muts.extractMutationsForRange(2, 11).mutate(seq1.getRange(2, 11)));
    }

    public int sAbs(int value) {
        if (value < 0)
            return -value - 1;
        return value;
    }

    @Test
    public void testGlobalRandom1LinearNucleotide() throws Exception {
        testGlobalRandom1(LinearGapAlignmentScoring.getNucleotideBLASTScoring());
    }

    @Test
    public void testGlobalRandom1LinearAminoAcid() throws Exception {
        testGlobalRandom1(LinearGapAlignmentScoring.getAminoAcidBLASTScoring(BLASTMatrix.BLOSUM62));
    }

    @Test
    public void testGlobalRandom1AffineNucleotide() throws Exception {
        testGlobalRandom1(AffineGapAlignmentScoring.getNucleotideBLASTScoring());
    }

    @Test
    public void testGlobalRandom1AffineAminoAcid() throws Exception {
        testGlobalRandom1(AffineGapAlignmentScoring.getAminoAcidBLASTScoring(BLASTMatrix.BLOSUM62));
    }

    public static <S extends Sequence<S>> void testGlobalRandom1(AlignmentScoring<S> sc) throws Exception {
        Alphabet<S> alphabet = sc.getAlphabet();
        Well19937c rdi = new Well19937c();
        RandomDataGenerator random = new RandomDataGenerator(rdi);
        int z;
        int iterations = its(1000, 1000000), checkFails = 0;
        for (int i = 0; i < iterations; ++i) {
            S seq1 = randomSequence(alphabet, rdi, 30, 40),
                    seq2 = randomSequence(alphabet, rdi, 30, 40),
                    seq3 = randomSequence(alphabet, rdi, 30, 40);

            Mutations<S> m1 = alignGlobal(sc, seq1, seq2).getAbsoluteMutations(),
                    m2 = alignGlobal(sc, seq2, seq3).getAbsoluteMutations();

            if (!(sc instanceof AffineGapAlignmentScoring)) {
                assertTrue(MutationsUtil.check(m1));
                assertTrue(MutationsUtil.check(m2));
            }

            //Testing move
            S seq4 = randomSequence(alphabet, rdi, 30, 40);
            assertEquals(seq2.concatenate(seq4), m1.mutate(seq1.concatenate(seq4)));
            assertEquals(seq4.concatenate(seq2), m1.move(seq4.size()).mutate(seq4.concatenate(seq1)));

            //Mutate method
            assertEquals(seq2, m1.mutate(seq1));

            //Mutate method for inverted mutations
            assertEquals(seq1, m1.invert().mutate(seq2));

            //Combine method
            Mutations<S> m3 = m1.combineWith(m2);
            //assertTrue(MutationsUtil.check(m3));

            assertEquals(seq3, m3.mutate(seq1));

            //Extract mutations
            int divPointsCount = random.nextInt(2, seq1.size() / 3);
            int[] divPoints = new int[divPointsCount];
            divPoints[0] = -1;
            divPoints[1] = seq1.size();
            for (z = 2; z < divPointsCount; z++)
                divPoints[z] = random.nextInt(0, seq1.size());
            Arrays.sort(divPoints);
            int totalMutations = 0;
            for (z = 1; z < divPointsCount; ++z)
                totalMutations += m1.extractMutationsForRange(divPoints[z - 1], divPoints[z]).size();

            assertEquals(m1.size(), totalMutations);

            for (z = 0; z < 100; ++z) {
                int from = random.nextInt(0, seq1.size() - 1);
                int to = random.nextInt(0, seq1.size() - from) + from;
                int from2 = m1.convertToSeq2Position(from);
                int to2 = m1.convertToSeq2Position(to);
                if (from2 >= 0 && to2 >= 0) {
                    //++to2;

                    if (to2 < from2)
                        to2 = from2;

                    assertEquals(seq2.getRange(from2, to2),
                            m1.extractMutationsForRange(from, to).mutate(seq1.getRange(from, to))
                    );
                    break;
                }
            }

            //Testing convertToSeq2Position
            TIntHashSet positions = new TIntHashSet();
            for (int j = 0; j < seq1.size(); ++j)
                positions.add(j);
            for (int mut : m1.getRAWMutations())
                positions.remove(Mutation.getPosition(mut));
            TIntIterator it = positions.iterator();
            int position;
            while (it.hasNext()) {
                position = it.next();
                assertEquals(seq1.codeAt(position),
                        seq2.codeAt(m1.convertToSeq2Position(position)));
            }
        }

        //System.out.println("Unchecked combine failed in " + (int) ((checkFails / (double) iterations) * 100) + "% cases");
    }

    @Test
    public void testLocal1() {
        NucleotideSequence ns1 = new NucleotideSequence("tccCAGTTATGTCAGgggacacgagcatgcagagac");
        NucleotideSequence ns2 = new NucleotideSequence("aattgccgccgtcgttttcagCAGTATGTCAGatc");

        Alignment<NucleotideSequence> r = Aligner.alignLocal(
                AffineGapAlignmentScoring.getNucleotideBLASTScoring(),
                ns1, ns2);

        Assert.assertEquals(3, r.getSequence1Range().getFrom());

        ns1 = new NucleotideSequence("CGTCCCGGG");
        ns2 = new NucleotideSequence("TCCC");
        r = Aligner.alignLocal(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), ns1, ns2);
        Assert.assertEquals(2, r.getSequence1Range().getFrom());

        ns1 = new NucleotideSequence("GGG");
        ns2 = new NucleotideSequence("CCC");
        Assert.assertEquals(null, Aligner.alignLocal(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), ns1, ns2));

        ns1 = new NucleotideSequence("GGCGCCAG");
        ns2 = new NucleotideSequence("CA");
        r = Aligner.alignLocal(AffineGapAlignmentScoring.getNucleotideBLASTScoring(), ns1, ns2);
        Assert.assertEquals(5, r.getSequence1Range().getFrom());

        ns1 = new NucleotideSequence("GGCGCCAG");
        ns2 = new NucleotideSequence("CA");
        r = Aligner.alignLocal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(), ns1, ns2);
        Assert.assertEquals(5, r.getSequence1Range().getFrom());

        ns1 = new NucleotideSequence("tccCAGTTATGTCAGgggacacgagcatgcagagac");
        ns2 = new NucleotideSequence("aattgccgccgtcgttttcagCAGTATGTCAGatc");
        r = Aligner.alignLocal(LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                ns1, ns2);
        Assert.assertEquals(0, r.getSequence1Range().getFrom());
    }

    @Test
    public void testLocalRandom1() {
        testLocalRandom(AffineGapAlignmentScoring.getNucleotideBLASTScoring());
    }

    @Test
    public void testLocalRandom2() {
        testLocalRandom(LinearGapAlignmentScoring.getNucleotideBLASTScoring());
    }

    public static void testLocalRandom(AlignmentScoring sc) {
        int its = TestUtil.its(100, 10000);
        Well19937c rdi = new Well19937c(1234112L);
        RandomDataGenerator generator = new RandomDataGenerator(rdi);
        for (int i = 0; i < its; ++i) {
            NucleotideSequence sq1 = randomSequence(NucleotideSequence.ALPHABET,
                    rdi, 100, 300);
            int length = generator.nextInt(10, 30);
            int from = generator.nextInt(0, sq1.size() - length - 1);
            NucleotideSequence needle = sq1.getRange(from, from + length);
            Alignment<NucleotideSequence> r = Aligner.alignLocal(sc, sq1, needle);
            Assert.assertEquals(from, r.getSequence1Range().getFrom());
            assertTrue(r.getAbsoluteMutations().isEmpty());
        }
    }

    @Test
    public void testLocalRandom4() {
        AlignmentScoring[] scorings = {LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                LinearGapAlignmentScoring.getAminoAcidBLASTScoring(BLASTMatrix.BLOSUM62),
                AffineGapAlignmentScoring.getNucleotideBLASTScoring(),
                AffineGapAlignmentScoring.getAminoAcidBLASTScoring(BLASTMatrix.BLOSUM62)};
        for (AlignmentScoring sc : scorings)
            testLocalRandomCheckAlignedSequences(sc);
    }

    public <S extends Sequence<S>> void testLocalRandomCheckAlignedSequences(AlignmentScoring<S> sc) {
        int its = TestUtil.its(100, 3000);
        Well19937c rdi = new Well19937c();
        for (int i = 0; i < its; ++i) {
            S sq1 = randomSequence(sc.getAlphabet(), rdi, 100, 300),
                    sq2 = randomSequence(sc.getAlphabet(), rdi, 100, 300);

            Alignment<S> r = Aligner.alignLocal(sc, sq1, sq2);

            Assert.assertEquals(sq2.getRange(r.getSequence2Range()),
                    r.getRelativeMutations().mutate(sq1.getRange(r.getSequence1Range())));
        }
    }

    @Test
    public void testLocalRandomNucleotide1() {
        AlignmentScoring<NucleotideSequence>[] scorings = new AlignmentScoring[]{
                LinearGapAlignmentScoring.getNucleotideBLASTScoring(),
                AffineGapAlignmentScoring.getNucleotideBLASTScoring()
        };
        for (AlignmentScoring sc : scorings)
            testLocalRandomCheckAlignedSequencesNucleotide(sc);
    }

    public void testLocalRandomCheckAlignedSequencesNucleotide(AlignmentScoring<NucleotideSequence> sc) {
        int its = TestUtil.its(100, 3000);
        Well19937c rand = new Well19937c();
        RandomDataGenerator rdi = new RandomDataGenerator(rand);

        GenericNucleotideMutationModel model = new GenericNucleotideMutationModel(
                SubstitutionModels.getUniformNucleotideSubstitutionModel(.05),
                .05, .05);

        // Local
        for (int i = 0; i < its; ++i) {
            NucleotideSequence sequence = randomSequence(sc.getAlphabet(), rand, 100, 300);

            int length = rdi.nextInt(20, 40);
            int from = rdi.nextInt(0, sequence.size() - length - 1);

            NucleotideSequence subsequence = sequence.getRange(from, from + length);

            Mutations<NucleotideSequence> mut = MutationsGenerator.generateMutations(subsequence, model);

            int expectedScoring = AlignmentUtils.calculateScore(sc, subsequence.size(), mut);

            subsequence = mut.mutate(subsequence);
            Alignment<NucleotideSequence> r = Aligner.alignLocal(sc, sequence, subsequence);
            Assert.assertTrue("Scoring.", r.score >= expectedScoring);

            Assert.assertEquals(r.getRelativeMutations().mutate(sequence.getRange(r.getSequence1Range())),
                    subsequence.getRange(r.getSequence2Range()));

            r = Aligner.alignLocal(sc, subsequence, sequence);

            Assert.assertEquals(r.getRelativeMutations().mutate(subsequence.getRange(r.getSequence1Range())),
                    sequence.getRange(r.getSequence2Range()));
        }

        // Global
        for (int i = 0; i < its; ++i) {
            NucleotideSequence seq1 = randomSequence(sc.getAlphabet(), rand, 100, 300);

            Mutations<NucleotideSequence> mut = MutationsGenerator.generateMutations(seq1, model);
            int expectedScoring = AlignmentUtils.calculateScore(sc, seq1.size(), mut);

            NucleotideSequence seq2 = mut.mutate(seq1);

            Alignment<NucleotideSequence> r = Aligner.alignGlobal(sc, seq1, seq2);
            Assert.assertEquals(r.getAbsoluteMutations().mutate(seq1), seq2);
            Assert.assertTrue("Scoring.", r.score >= expectedScoring);

            r = Aligner.alignGlobal(sc, seq2, seq1);
            Assert.assertEquals(r.getAbsoluteMutations().mutate(seq2), seq1);
        }
    }

    @Test
    public void testLocalRandomCheckNucleotideSCoring() {
        LinearGapAlignmentScoring sc = LinearGapAlignmentScoring.getNucleotideBLASTScoring();

        int its = TestUtil.its(100, 5000);
        Well19937c rand = new Well19937c();
        RandomDataGenerator rdi = new RandomDataGenerator(rand);

        GenericNucleotideMutationModel model = new GenericNucleotideMutationModel(
                SubstitutionModels.getUniformNucleotideSubstitutionModel(.05),
                .05, .05);

        for (int i = 0; i < its; ++i) {
            NucleotideSequence sequence = randomSequence(NucleotideSequence.ALPHABET, rand, 100, 300);

            int length = rdi.nextInt(20, 40);
            int from = rdi.nextInt(0, sequence.size() - length - 1);

            NucleotideSequence subsequence = sequence.getRange(from, from + length);

            Mutations<NucleotideSequence> mut = MutationsGenerator.generateMutations(subsequence, model);
            float mutScore = AlignmentUtils.calculateScore(sc, subsequence.size(), mut);

            subsequence = mut.mutate(subsequence);

            Alignment<NucleotideSequence> r = Aligner.alignLocal(sc, sequence, subsequence);

            Assert.assertEquals(r.getRelativeMutations().mutate(sequence.getRange(r.getSequence1Range())),
                    subsequence.getRange(r.getSequence2Range()));

            Assert.assertTrue(mutScore <= AlignmentUtils.calculateScore(sc, r.getSequence1Range().length(), r.getRelativeMutations()));

            r = Aligner.alignLocal(sc, subsequence, sequence);

            Assert.assertEquals(r.getRelativeMutations().mutate(subsequence.getRange(r.getSequence1Range())),
                    sequence.getRange(r.getSequence2Range()));

            Assert.assertTrue(mutScore <= AlignmentUtils.calculateScore(sc, r.getSequence1Range().length(), r.getRelativeMutations()));
        }
    }

    @Test
    public void testSerialization1() throws Exception {
        NucleotideSequence seq1 = new NucleotideSequence("ATTAGACA"),
                seq2 = new NucleotideSequence("ACAGATAC");

        Alignment se = Aligner.alignOnlySubstitutions(seq1, seq2);
        IOTestUtil.assertJavaSerialization(se);
    }

//    if (alphabet.getAlphabetName().equals("nucleotide")) {
//        // Indel shift
//        shiftIndelsAtHomopolymers(seq1, m1);
//        Assert.assertEquals("Indel shift don't change sequence", seq2, Mutations.mutate(seq1, m1));
//
//        // Mutation filter
//        int[] m4 = combineMutations(m1, Mutations.invertMutations(m1));
//        m4 = filterMutations(seq1, m4);
//
//        Assert.assertEquals("Filter removed all non-informative mutations", 0, m4.length);
//
//
//        // Mutation filter
//        // TODO: good test for false-positives
//        //int[] m5 = Mutations.generateMutations((NucleotideSequence) seq1,
//        //        MutationModels.getEmpiricalNucleotideMutationModel());
//        //int[] m6 = filterMutations(seq1, m5);
//
//        //if (m5.length != m6.length) {
//        //    System.out.println(seq1);
//        //    Mutations.printMutations(NucleotideAlphabet.INSTANCE, m5);
//        //    System.out.println();
//        //    Mutations.printMutations(NucleotideAlphabet.INSTANCE, m6);
//        //    System.out.println();
//        //}
//
//        //Assert.assertEquals("No real mutations lost", m5.length, m6.length);
//    }
}