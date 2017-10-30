package com.milaboratory.core.alignment;

import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive;
import org.junit.Test;

import static com.milaboratory.core.alignment.PatternAndTargetAligner.*;
import static com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive.fromNucleotideSequence;
import static org.junit.Assert.*;

public class PatternAndTargetAlignerTest {
    private static final PatternAndTargetAlignmentScoring simpleScoring = new PatternAndTargetAlignmentScoring(
            0, -9, -10, false, (byte)0, (byte)0, 0);

    private static void assertScore(int expected, float score) {
        assertEquals((float)expected, score, 0.001);
    }

    @Test
    public void testGlobal() throws Exception {
        assertScore(-19, alignGlobal(simpleScoring,
                new NucleotideSequenceCaseSensitive("ATTagaca"), new NSequenceWithQuality("ATTAAGA")).getScore());
        assertScore(-100000009, alignGlobal(simpleScoring,
                new NucleotideSequenceCaseSensitive("ATTAGACA"), new NSequenceWithQuality("ATTAAGA")).getScore());

        NucleotideSequenceCaseSensitive seq1 = new NucleotideSequenceCaseSensitive("ATttTAtaCa");
        NSequenceWithQuality seq2 = new NSequenceWithQuality("GGGAGGCATTAGACCAAT");
        assertEquals(fromNucleotideSequence(seq2.getSequence(), true),
                alignGlobal(simpleScoring, seq1, seq2).getAbsoluteMutations().mutate(seq1));

        NucleotideSequenceCaseSensitive seq3 = new NucleotideSequenceCaseSensitive("TGTC");
        NSequenceWithQuality seq4 = new NSequenceWithQuality("ACCTTTATTGACCAGGATTGCAGGACGGCCAGCCAG");
        assertEquals(fromNucleotideSequence(seq4.getSequence(), true),
                alignGlobal(simpleScoring, seq3, seq4).getAbsoluteMutations().mutate(seq3));
    }

    @Test
    public void testGlobalMove() throws Exception {
        NucleotideSequenceCaseSensitive seq1 = new NucleotideSequenceCaseSensitive("tagaattaGACA");
        NSequenceWithQuality seq2 = new NSequenceWithQuality("ATTAGTACA");
        NucleotideSequenceCaseSensitive s = new NucleotideSequenceCaseSensitive("gACATAC");
        Alignment<NucleotideSequenceCaseSensitive> a = alignGlobal(simpleScoring, seq1, seq2);
        Mutations<NucleotideSequenceCaseSensitive> muts = a.getAbsoluteMutations();
        assertEquals(fromNucleotideSequence(seq2.getSequence(), true).concatenate(s),
                muts.mutate(seq1.concatenate(s)));
        assertEquals(s.concatenate(fromNucleotideSequence(seq2.getSequence(), true)),
                muts.move(s.size()).mutate(s.concatenate(seq1)));
    }

    @Test
    public void testGlobalPosition() throws Exception {
        NucleotideSequenceCaseSensitive seq1 = new NucleotideSequenceCaseSensitive("tagaattagaca");
        NSequenceWithQuality seq2 = new NSequenceWithQuality("ATTAGTAA");

        Alignment<NucleotideSequenceCaseSensitive> a = alignGlobal(new PatternAndTargetAlignmentScoring(0,
                -10, -9, false, (byte)0, (byte)0, 0), seq1, seq2);
        Mutations<NucleotideSequenceCaseSensitive> muts = a.getAbsoluteMutations();

        int[] v = {-1, -1, -1, -1, 0, 1, 2, 3, 4, 6, -8, 7};
        for (int i = 0; i < seq1.size(); ++i)
            assertEquals(v[i], muts.convertToSeq2Position(i));
    }
}
