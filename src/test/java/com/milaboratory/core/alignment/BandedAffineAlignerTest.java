package com.milaboratory.core.alignment;

import com.milaboratory.core.Range;
import com.milaboratory.core.mutations.MutationsBuilder;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.Sequence;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Assert;
import org.junit.Test;

import static com.milaboratory.test.TestUtil.its;
import static com.milaboratory.test.TestUtil.randomSequence;
import static org.junit.Assert.assertTrue;

/**
 * Created by poslavsky on 20/10/15.
 */
public class BandedAffineAlignerTest {
    @Test
    public void test1() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 1, -10, -3, -1);

        NucleotideSequence a = new NucleotideSequence("ataaaaaaatgatcgacaaaaaaaatttttttt");
        NucleotideSequence b = new NucleotideSequence("agtcgttagcgacaaaaaaa");

        a = new NucleotideSequence("atcgagctagttttttttttt");
        b = new NucleotideSequence("ataaaaaaaaaaacgagctag");

        MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        BandedAffineAligner.align0(scoring, a, b, 0, a.size(), 0, b.size(), 151, mutations, new BandedAffineAligner.MatrixCache());

        System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
        System.out.println(Aligner.alignGlobalAffine(scoring, a, b));
        //System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
    }

    @Test
    public void test2() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        NucleotideSequence a = new NucleotideSequence("ataaaaaaatgatcgacaaaaaaaatttttttt");
        NucleotideSequence b = new NucleotideSequence("agtcgttagcgacaaaaaaa");

        a = new NucleotideSequence("atgcggggatgc");
        b = new NucleotideSequence("atgctaatgcttttttttttt");

        MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        BandedSemiLocalResult res = BandedAffineAligner.semiLocalRight0(scoring, a, b, 0, a.size(), 0, b.size(), 2, mutations, new BandedAffineAligner.MatrixCache());

        //BandedAffineAligner.align0(scoring, a, b, 0, a.size(), 0, b.size(), 151, mutations, new BandedAffineAligner.MatrixCache());

        System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, res.sequence1Stop + 1), new Range(0, res.sequence2Stop + 1), 100));
        System.out.println(Aligner.alignGlobalAffine(scoring, a, b));
        //System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
    }

    @Test
    public void test23() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        NucleotideSequence a = new NucleotideSequence("ataaaaaaatgatcgacaaaaaaaatttttttt");
        NucleotideSequence b = new NucleotideSequence("agtcgttagcgacaaaaaaa");

        a = new NucleotideSequence("atgcggggatgc");
        b = new NucleotideSequence("atgcggggatgc");

        MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        BandedSemiLocalResult res = BandedAffineAligner.semiLocalRight0(scoring, a, b, 0, a.size(), 0, b.size(), 2, mutations, new BandedAffineAligner.MatrixCache());
        //BandedSemiLocalResult res = BandedLinearAligner.alignSemiLocalLeft0(
        //        new LinearGapAlignmentScoring<>(NucleotideSequence.ALPHABET, 5, -2, -4), a, b, 0, a.size(), 0,
        //        b.size(), 2, -1000, mutations, new CachedIntArray());

        //BandedAffineAligner.align0(scoring, a, b, 0, a.size(), 0, b.size(), 151, mutations, new BandedAffineAligner.MatrixCache());

        System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, res.sequence1Stop + 1), new Range(0, res.sequence2Stop + 1), 100));

        //System.out.println(Aligner.alignGlobalAffine(scoring, a, b));
        //System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
    }

    @Test
    public void test3() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        NucleotideSequence a = new NucleotideSequence("ataaaaaaatgatcgacaaaaaaaatttttttt");
        NucleotideSequence b = new NucleotideSequence("agtcgttagcgacaaaaaaa");

        a = new NucleotideSequence("cgtaggggcgta");
        b = new NucleotideSequence("tttttttttttcgtaatcgta");

        MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        BandedSemiLocalResult res = BandedAffineAligner.semiLocalLeft0(scoring, a, b, 0, a.size(), 0, b.size(), 2, mutations, new BandedAffineAligner.MatrixCache());

        //BandedAffineAligner.align0(scoring, a, b, 0, a.size(), 0, b.size(), 151, mutations, new BandedAffineAligner.MatrixCache());

        System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(res.sequence1Stop, a.size()), new Range(res.sequence2Stop, b.size()), 100));
        System.out.println(Aligner.alignGlobalAffine(scoring, a, b));
        //System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
    }

    @Test
    public void test4() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        NucleotideSequence a = new NucleotideSequence("ataaaaaaatgatcgacaaaaaaaatttttttt");
        NucleotideSequence b = new NucleotideSequence("agtcgttagcgacaaaaaaa");

        //a = new NucleotideSequence("atgcggggatgc");
        //b = new NucleotideSequence("atgctaatgcttttttttttt");
        a = new NucleotideSequence("atgcggggatgttttttt");
        b = new NucleotideSequence("atgcggggatag");


        MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        BandedSemiLocalResult res = BandedAffineAligner.semiGlobalRight0(scoring, a, b,
                0, a.size(), 2,
                0, b.size(), 2,
                2, mutations, new BandedAffineAligner.MatrixCache());

        //BandedAffineAligner.align0(scoring, a, b, 0, a.size(), 0, b.size(), 151, mutations, new BandedAffineAligner.MatrixCache());

        System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, res.sequence1Stop + 1), new Range(0, res.sequence2Stop + 1), 100));
        System.out.println(Aligner.alignGlobalAffine(scoring, a, b));
        //System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
    }

    @Test
    public void test5() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        NucleotideSequence a = new NucleotideSequence("ataaaaaaatgatcgacaaaaaaaatttttttt");
        NucleotideSequence b = new NucleotideSequence("agtcgttagcgacaaaaaaa");

        //a = new NucleotideSequence("atgcggggatgc");
        //b = new NucleotideSequence("atgctaatgcttttttttttt");
        a = new NucleotideSequence("tttttttgtaggggcgta");
        b = new NucleotideSequence("gataggggcgta");


        MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
        BandedSemiLocalResult res = BandedAffineAligner.semiGlobalLeft0(scoring, a, b,
                3, a.size() - 3, 6,
                0, b.size(), 2,
                2, mutations, new BandedAffineAligner.MatrixCache());

        //BandedAffineAligner.align0(scoring, a, b, 0, a.size(), 0, b.size(), 151, mutations, new BandedAffineAligner.MatrixCache());

        System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(res.sequence1Stop, a.size()), new Range(res.sequence2Stop, b.size()), 100));
        System.out.println();
        System.out.println(Aligner.alignGlobalAffine(scoring, a, b));
        //System.out.println(new Alignment<>(a, mutations.createAndDestroy(), new Range(0, a.size()), new Range(0, b.size()), 100));
    }


    @Test
    public void testSemiGlobalRightRandom1() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        int its = its(10000, 100000);
        NucleotideSequence seq1, seq2;
        int offset1, offset2, length1, length2, added1, added2;
        Alignment<NucleotideSequence> la;
        RandomDataGenerator random = new RandomDataGenerator(new Well19937c());
        for (int i = 0; i < its; ++i) {
            seq1 = randomSequence(NucleotideSequence.ALPHABET, random, 80, 84);
            seq2 = randomSequence(NucleotideSequence.ALPHABET, random, 80, 84);
            offset1 = random.nextInt(0, seq1.size() - 10);
            offset2 = random.nextInt(0, seq2.size() - 10);
            length1 = random.nextInt(1, seq1.size() - offset1);
            length2 = random.nextInt(1, seq2.size() - offset2);
            added1 = random.nextInt(0, length1);
            added2 = random.nextInt(0, length2);
            la = BandedAffineAligner.semiGlobalRight(scoring,
                    seq1, seq2, offset1, length1, added1, offset2, length2, added2, 1);

            assertTrue(la.getSequence1Range().getTo() == offset1 + length1 ||
                    la.getSequence2Range().getTo() == offset2 + length2);

            assertTrue(la.getSequence1Range().getTo() >= offset1 + length1 - added1);
            assertTrue(la.getSequence2Range().getTo() >= offset2 + length2 - added2);

            assertAlignment(la, seq2);
        }
    }

    @Test
    public void testSemiGlobalLeftRandom1() throws Exception {
        AffineGapAlignmentScoring<NucleotideSequence> scoring = new AffineGapAlignmentScoring<>(
                NucleotideSequence.ALPHABET, 3, -1, -3, -1);

        int its = its(1000, 100000);
        NucleotideSequence seq1, seq2;
        int offset1, offset2, length1, length2, added1, added2;
        Alignment<NucleotideSequence> la;
        RandomDataGenerator random = new RandomDataGenerator(new Well19937c());
        for (int i = 0; i < its; ++i) {
            seq1 = randomSequence(NucleotideSequence.ALPHABET, random, 80, 84);
            seq2 = randomSequence(NucleotideSequence.ALPHABET, random, 80, 84);
            offset1 = random.nextInt(0, seq1.size() - 10);
            offset2 = random.nextInt(0, seq2.size() - 10);
            length1 = random.nextInt(1, seq1.size() - offset1);
            length2 = random.nextInt(1, seq2.size() - offset2);
            added1 = random.nextInt(0, length1);
            added2 = random.nextInt(0, length2);
            la = BandedAffineAligner.semiGlobalLeft(scoring,
                    seq1, seq2, offset1, length1, added1, offset2, length2, added2, 1);

            assertTrue(la.getSequence1Range().getFrom() == offset1 ||
                    la.getSequence2Range().getFrom() == offset2);
            assertTrue(la.getSequence1Range().getFrom() <= offset1 + added1);
            assertTrue(la.getSequence2Range().getFrom() <= offset2 + added2);

            assertAlignment(la, seq2);
        }
    }

    static <T extends Sequence<T>> void assertAlignment(Alignment<T> alignment, T s2) {
        Assert.assertEquals(
                alignment.getRelativeMutations().mutate(alignment.sequence1.getRange(
                        alignment.getSequence1Range())), s2.getRange(alignment.getSequence2Range()));
    }
}