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

import com.milaboratory.core.alignment.AffineGapAlignmentScoring;
import com.milaboratory.core.alignment.Aligner;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.sequence.*;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well44497a;
import org.junit.Assert;
import org.junit.Test;

import static com.milaboratory.core.mutations.MutationsUtil.btopDecode;
import static com.milaboratory.core.mutations.MutationsUtil.nt2aa;
import static com.milaboratory.core.sequence.TranslationParameters.FromLeftWithIncompleteCodon;
import static com.milaboratory.core.sequence.TranslationParameters.FromRightWithIncompleteCodon;

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

    public static final AffineGapAlignmentScoring<NucleotideSequence> SCORE_NT2AA = new AffineGapAlignmentScoring<>(
            NucleotideSequence.ALPHABET, 1, -3, -5, -2);

    @Test
    public void testNT2AA1() throws Exception {
        //                                                   F  R  H  R  L  Q
        NucleotideSequence ntSeq1 = new NucleotideSequence("TTTAGACACAGATTGCAA");

        NucleotideSequence ntSeq2;

        //                                F  Shift..
        ntSeq2 = new NucleotideSequence("TTTAGCACAGATCGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, null, FromLeftWithIncompleteCodon);

        //                                F  R  R  S  Q
        ntSeq2 = new NucleotideSequence("TTTAGAAGATCGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, "DH2 SL4S", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATCGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, "SL4S", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "DQ5", FromLeftWithIncompleteCodon);

        //                                R  H  R  L
        ntSeq2 = new NucleotideSequence("AGACACAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "DF0 DQ5", FromLeftWithIncompleteCodon);

        //                                *  H  R  L
        ntSeq2 = new NucleotideSequence("TGACACAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "SF0* DR1 DQ5", FromLeftWithIncompleteCodon);

        //                                R  H  L  R  L
        ntSeq2 = new NucleotideSequence("AGACACTTGAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "DF0 I3L DQ5", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTAGACACCGTTTGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, "", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L
        ntSeq2 = new NucleotideSequence("TTTAGACACCGTTTA");
        nt2aaAssert(ntSeq1, ntSeq2, "DQ5", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L  Q  F
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTGCAATTT");
        nt2aaAssert(ntSeq1, ntSeq2, "I6F", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L  Q  _
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTGCAATT");
        nt2aaAssert(ntSeq1, ntSeq2, "I6_", FromLeftWithIncompleteCodon);

        //                                F  R  H  R  L  _
        ntSeq1 = new NucleotideSequence("TTTAGACACAGATTGCA");
        //                                F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTGCAA");
        nt2aaAssert(ntSeq1, ntSeq2, "S_5Q", FromLeftWithIncompleteCodon);
    }

    @Test
    public void testNT2AA2() throws Exception {
        //                                                   F  R  H  R  L  Q
        NucleotideSequence ntSeq1 = new NucleotideSequence("TTTAGACACAGATTGCAA");

        NucleotideSequence ntSeq2;

        //                                 Shift..
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATGCAA");
        nt2aaAssert(ntSeq1, ntSeq2, null, FromRightWithIncompleteCodon);

        //                                F  R  R  S  Q
        ntSeq2 = new NucleotideSequence("TTTAGAAGATCGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, "DH2 SL4S", FromRightWithIncompleteCodon);

        //                                F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATCGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, "SL4S", FromRightWithIncompleteCodon);

        //                                F  R  H  R  L
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "DQ5", FromRightWithIncompleteCodon);

        //                                R  H  R  L
        ntSeq2 = new NucleotideSequence("AGACACAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "DF0 DQ5", FromRightWithIncompleteCodon);

        //                                *  H  R  L
        ntSeq2 = new NucleotideSequence("TGACACAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "SF0* DR1 DQ5", FromRightWithIncompleteCodon);

        //                                R  H  L  R  L
        ntSeq2 = new NucleotideSequence("AGACACTTGAGATTG");
        nt2aaAssert(ntSeq1, ntSeq2, "DF0 I3L DQ5", FromRightWithIncompleteCodon);

        //                                F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTAGACACCGTTTGCAG");
        nt2aaAssert(ntSeq1, ntSeq2, "", FromRightWithIncompleteCodon);

        //                                F  R  H  R  L
        ntSeq2 = new NucleotideSequence("TTTAGACACCGTTTA");
        nt2aaAssert(ntSeq1, ntSeq2, "DQ5", FromRightWithIncompleteCodon);

        //                                F  R  H  R  L  Q  F
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTGCAATTT");
        nt2aaAssert(ntSeq1, ntSeq2, "I6F", FromRightWithIncompleteCodon);

        //                               _  F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("AATTTAGACACAGATTGCAA");
        nt2aaAssert(ntSeq1, ntSeq2, "I0_", FromRightWithIncompleteCodon);

        //                               _  F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTTTAGACACAGATTGCAA");
        nt2aaAssert(ntSeq1, ntSeq2, "I0_", FromRightWithIncompleteCodon);

        //
        //                               _  F  R  H  R  L  Q
        ntSeq1 = new NucleotideSequence("AATTTAGACACAGATTGCAA");
        //                                F  R  H  R  L  Q
        ntSeq2 = new NucleotideSequence("TTTAGACACAGATTGCAA");
        nt2aaAssert(ntSeq1, ntSeq2, "D_0", FromRightWithIncompleteCodon);
    }

    private void nt2aaAssert(NucleotideSequence ntSeq1, NucleotideSequence ntSeq2,
                             String expectedMutations, TranslationParameters tt) {
        Alignment<NucleotideSequence> al = null;
        AminoAcidSequence aaSeq1 = null, aaSeq2 = null;
        try {
            al = Aligner.alignGlobal(SCORE_NT2AA, ntSeq1, ntSeq2);

            aaSeq1 = AminoAcidSequence.translate(ntSeq1, tt);
            aaSeq2 = AminoAcidSequence.translate(ntSeq2, tt);

            Mutations<AminoAcidSequence> aaMutations = nt2aa(ntSeq1, al.getAbsoluteMutations(), tt, 2);
            if (expectedMutations != null) {
                Mutations<AminoAcidSequence> expectedMutationsM = Mutations.decode(expectedMutations, AminoAcidSequence.ALPHABET);
                Assert.assertEquals("Checking assert input.", aaSeq2, expectedMutationsM.mutate(aaSeq1));
                Assert.assertEquals("Actual assert.", expectedMutationsM, aaMutations);
            } else {
                Assert.assertNull(aaMutations);
            }
        } catch (Throwable e) {
            System.out.println(al.getAlignmentHelper());
            System.out.println(aaSeq2);
            throw e;
        }
    }

    @Test
    public void test5() throws Exception {
        String btop = "22TG23A-G-A-A-A-2A-G-A-G-1T-TC2T-CATA2C-2A-C-6GA23TCTC1G-CA3TC1-A13TCCA1TG6GT1GTAG13AGCA2T-1T-T-T-T-T-1T-3G-C-2A-A-A-A-A-1A-16C-3-A4AG8TAAG26CT16TCAT8TC11TC71GC4CG11AC10AT4AT20AG1TA5AGGCTC1GAGT10GA5CGCT1AGCG4AG4TC10CT24GA4AG2TA7CT37AGATCT4TGGA13AG5TC4CTTC9CTCTCTAC1AT3AGTGTC5AT3AT3-G-G-A-G-A3AG2CT4TC56GA2TC5AC4TA9AG64GA6ACTC2TC3-A-A2T-2A-A-2-T1-C2AG3TG20CT87GA8AC14CT8AGCG1-A2TA2C-1A-15TCTC5GC2TC2C-2G-3GT1-C1TC73AGTC2TC9GT2G-3GA1CT14AGAG18-A39TC36GAAT3ACTA2AG1TA10TG16AG18TCTA14AT9CT17CT1TG9AC1GA45AT11CT4TC4AGTC1GC2AC1TC15AG3CG1AGGA20CG21";
        String qseqStr = "CTCAGAACGAACGCTGGCGGCATGCCTAACACATGCAAGTCGAACGAGAAACCAGAGCTTGCTCTGGCGGACAGTGGCGGACGGGTGAGTAACGCGTGGGAATTTGCCCTTG-GGTACGGAACAACTCATGGAAACGTGAGCTAATACCGTATACGCTCTTTTTCTTTAGCGGAAAAAGAGGAAAGATTTATCGCCCTTG-GATGAGCCCGCGTTAGATTAGCTAGTTGGTGAGGTAATGGCCCACCAAGGCGACGATCTATAGCTGGTTTGAGAGGATGATCAGCCACACTGGGACTGAGACACGGCCCAGACTCCTACGGGAGGCAGCAGTGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCAATGCCGCGTGAGTGAAGAAGGCCTTAGGGTTGTAAAACTCTTTCAGTGGGGAAGATAATGGCGGTACCCACAGAAAAAGCTCCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGAGGGAGCTAGCGTTGCTCGGAATTACTGGGCGTAAAGCGTACGTAGGCGGATCAACAAGTTGGGGGTGAAATCCCAGGGCTTAACCCTGGAACTGCCCCCAAAACTATTGATCTAGAGACCG-----GGTAAGCGGAATTCCTAGTGTAGAGGTGAAATTCGTAGATATTAGGAAGAACACCAGTGGCGAAGGCGGCTTACTGGACCGGTACTGACGCTAAGGTACGAAAGCGTGGGGAGCAAACAGGATTAGATACCCTGGTAGTCCACGCCGTAAACGATGGGTGCTAGATGTTGGG--GCTTTAAGC-T-TCAGTGTCGCAGCTAACGCATTAAGCACCCCGCCTGGGGAGTACGGTCGCAAGATTAAAACTCAAAGGAATTGACGGGGGCCCGCACAAGCGGTGGAGCATGTGGTTTAATTCGAGGCAACGCGAAGAACCTTACCAGCCCTTGACATACC-GGTCGCGATTTCCAGAGATGGATTTCTTCAGTTTGGCTGGACCGG-ATACAGGTGCTGCATGGCTGTCGTCAGCTCGTGTCGTGAGATGTTGGGTTAAGTCCCGCAACGAGCGCAACCCTCATCTTTAGTTGCCAGCAGTTCGGCTGGGCACTCTAGAGAAACTGCCGGTGATAAGCCG-GAGGAAGGTGGGGATGACGTCAAGTCCTCATGGCCCTTATGGGCTGGGCTACACACGTGCTACAATGGCGGTGACAGAGGGATGCAATGGGGCGACCCTGAGCTAATCTCAAAAAACCGTCTCAGTTCGGATTGTTCTCTGCAACTCGAGAGCATGAAGTCGGAATCGCTAGTAATCGCGTATCAGCATGACGCGGTGAATACGTTCCCGGGCCTTGTACACACCGCCCGTCACACCAAGGGAGTTGGTTCTACCTGAAGATGGTGAGTTAACCCGCAAGGGAGACAGCCAGCCACGGTAGGGTCAGCGACTCGGGTGAAGTCGTAACAAGGTA";
        String sseqStr = "CTCAGAACGAACGCTGGCGGCAGGCCTAACACATGCAAGTCGAACG-----CC----C-CGC-AAGG-GG--AGTGGCAGACGGGTGAGTAACGCGTGGGAACCT-ACCTCGAGGTACGGAACAACCAAGGGAAACTTTGGCTAATACCGTATGAGC-C-----C-TTA--GG-----G-GGAAAGATTTATCGCC-TTGAGATGGGCCCGCGTAGGATTAGCTAGTTGGTGAGGTAATGGCTCACCAAGGCGACGATCCTTAGCTGGTCTGAGAGGATGACCAGCCACACTGGGACTGAGACACGGCCCAGACTCCTACGGGAGGCAGCAGTGGGGAATATTGGACAATGGGCGCAAGCCTGATCCAGCCATGCCGCGTGTGTGATGAAGGCCTTAGGGTTGTAAAGCACTTTCGCCGATGAAGATAATGACGGTAGTCGGAGAAGAAGCCCCGGCTAACTTCGTGCCAGCAGCCGCGGTAATACGAAGGGGGCAAGCGTTGTTCGGAATTACTGGGCGTAAAGCGTACGTAGGCGGATCGTTAAGTGAGGGGTGAAATCCCGGGGCTCAACCTCGGAACTGCCTTTCATACTGGCGATCTTGAGTCCGGGAGAGGTGAGTGGAACTCCTAGTGTAGAGGTGAAATTCGTAGATATTAGGAAGAACACCAGTGGCGAAGGCGACTCACTGGCCCGGAACTGACGCTGAGGTACGAAAGCGTGGGGAGCAAACAGGATTAGATACCCTGGTAGTCCACGCCGTAAACGATGGATGCTAGCCGTCGGGAAGC-TT--GCTTCTCGGTGGCGCAGCTAACGCATTAAGCATCCCGCCTGGGGAGTACGGTCGCAAGATTAAAACTCAAAGGAATTGACGGGGGCCCGCACAAGCGGTGGAGCATGTGGTTTAATTCGAAGCAACGCGCAGAACCTTACCAGCTCTTGACATGGCAGGACG-G-TTTCCAGAGATGGATCCCTTCACTTCGG-TG-ACCTGCACACAGGTGCTGCATGGCTGTCGTCAGCTCGTGTCGTGAGATGTTGGGTTAAGTCCCGCAACGAGCGCAACCCTCGCCTCTAGTTGCCATCA-TTCAGTTGGGCACTCTAGAGGGACTGCCGGTGATAAGCCGAGAGGAAGGTGGGGATGACGTCAAGTCCTCATGGCCCTTACGGGCTGGGCTACACACGTGCTACAATGGCGGTGACAATGGGCAGCGAAGGGGCGACCCGGAGCTAATCTCAAAAAGCCGTCTCAGTTCGGATTGCACTCTGCAACTCGAGTGCATGAAGTTGGAATCGCTAGTAATCGTGGATCAGCATGCCACGGTGAATACGTTCCCGGGCCTTGTACACACCGCCCGTCACACCATGGGAGTTGGTTTTACCCGAAGGCGCTGCGCTAACCCGCAAGGGAGGCAGGCGACCACGGTAGGGTCAGCGACTGGGGTGAAGTCGTAACAAGGTA";
        NucleotideSequence qseq = new NucleotideSequence(qseqStr.replace("-", "")), sseq = new NucleotideSequence(sseqStr.replace("-", ""));
        Mutations muts = new Mutations<>(NucleotideSequence.ALPHABET, btopDecode(btop, NucleotideSequence.ALPHABET));
        Assert.assertEquals(qseq, muts.mutate(sseq));
    }

    @Test
    public void test6() throws Exception {
        String btop = "6ATCT38A-1AT3-G1AG1-G1GSCT6-C5G-2-G23".replace("S", "N");
        String qseqStr = "CTCAGAACGAACGCTGGCGGCATGCCTAACACATGCAAGTCGAACGAGAAACCAGAGCTTGCTCTGGCGGACAGTGGCGGACGGGTGAGTAACGC".replace("S", "N");
        String sseqStr = "CTCAGATTGAACGCTGGCGGCATGCCTAACACATGCAAGTCGAACGGTAACGCGGGASTTTGCTCCTGGCGACGAGTGGCGGACGGGTGAGTAACGC".replace("S", "N");
        NucleotideSequence qseq = new NucleotideSequence(qseqStr.replace("-", "")), sseq = new NucleotideSequence(sseqStr.replace("-", ""));
        Mutations muts = new Mutations<>(NucleotideSequence.ALPHABET, btopDecode(btop, NucleotideSequence.ALPHABET));
        Assert.assertEquals(qseq, muts.mutate(sseq));
    }

    @Test
    public void test7() throws Exception {
        String qseqStr = "CTCAGAACGAACGCTGGCGGCATGCCTAACACATGCAAGTCGAACGAGAAACCAGAGCTTGCTCTGGCGGACAGTGGCGGACGGGTGAGTAACGC".replace("S", "N");
        String sseqStr = "CTCAGATTGAACGCTGGCGGCATGCCTAACACATGCAAGTCGAACGGTAACGCGGGASTTTGCTCCTGGCGACGAGTGGCGGACGGGTGAGTAACGC".replace("S", "N");
        String btop = "6ATCT38A-1AT3-G1AG1-G1GSCT6-C5G-2-G23".replace("S", "N");
        NucleotideSequence qseq = new NucleotideSequence(qseqStr.replace("-", "")), sseq = new NucleotideSequence(sseqStr.replace("-", ""));
        Mutations muts = new Mutations<>(NucleotideSequence.ALPHABET, btopDecode(btop, NucleotideSequence.ALPHABET));
        Assert.assertEquals(qseq, muts.mutate(sseq));
    }
}