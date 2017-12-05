package com.milaboratory.core.alignment;

import com.milaboratory.core.io.util.IOTestUtil;
import com.milaboratory.core.sequence.NucleotideAlphabetCaseSensitive;
import com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive;
import com.milaboratory.util.GlobalObjectMappers;
import org.junit.Test;

import static com.milaboratory.core.sequence.SequenceQuality.BAD_QUALITY_VALUE;
import static com.milaboratory.core.sequence.SequenceQuality.GOOD_QUALITY_VALUE;
import static org.junit.Assert.*;

public class PatternAndTargetAlignmentScoringTest {
    @Test
    public void serializationTest() throws Exception {
        PatternAndTargetAlignmentScoring expected = new PatternAndTargetAlignmentScoring(0, -1,
                -1, GOOD_QUALITY_VALUE, BAD_QUALITY_VALUE, -1);
        String s = GlobalObjectMappers.PRETTY.writeValueAsString(expected);
        PatternAndTargetAlignmentScoring scoring = GlobalObjectMappers.ONE_LINE.readValue(s,
                PatternAndTargetAlignmentScoring.class);
        assertEquals(expected, scoring);
        IOTestUtil.assertJavaSerialization(expected);
        IOTestUtil.assertJavaSerialization(scoring);
    }

    @Test
    public void testWildcard() throws Exception {
        PatternAndTargetAlignmentScoring scoring = new PatternAndTargetAlignmentScoring(0, -5,
                -10, GOOD_QUALITY_VALUE, BAD_QUALITY_VALUE, 0);

        assertEquals(0, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.A));
        assertEquals(0, scoring.getScore(NucleotideAlphabetCaseSensitive.a,
                NucleotideAlphabetCaseSensitive.A));
        assertEquals(-5, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.T));
        assertEquals(-5, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.t));

        assertEquals(-2, scoring.getScore(NucleotideAlphabetCaseSensitive.c,
                NucleotideAlphabetCaseSensitive.S));
        assertEquals(-2, scoring.getScore(NucleotideAlphabetCaseSensitive.S,
                NucleotideAlphabetCaseSensitive.s));

        assertEquals(-5, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.S));
        assertEquals(-5, scoring.getScore(NucleotideAlphabetCaseSensitive.W,
                NucleotideAlphabetCaseSensitive.S));
    }

    @Test
    public void testScoreWithQuality() throws Exception {
        PatternAndTargetAlignmentScoring scoring = new PatternAndTargetAlignmentScoring(0, -5,
                -10, (byte)12, (byte)2, -10);
        assertEquals(0, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.A, (byte)20));
        assertEquals(-5, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.A, (byte)7));
        assertEquals(-10, scoring.getScore(NucleotideAlphabetCaseSensitive.A,
                NucleotideAlphabetCaseSensitive.A, (byte)1));
        assertEquals(-3, scoring.getScore(NucleotideAlphabetCaseSensitive.S,
                NucleotideAlphabetCaseSensitive.s, (byte)11));
    }

    @Test
    public void testGapPenalty() throws Exception {
        PatternAndTargetAlignmentScoring scoring = new PatternAndTargetAlignmentScoring(0, -5,
                -10, GOOD_QUALITY_VALUE, BAD_QUALITY_VALUE, 0);
        NucleotideSequenceCaseSensitive pattern = new NucleotideSequenceCaseSensitive("aTTagaCA");
        int expectedPenalties[] = new int[] {-10, -1000000, -1000000, -1000000, -1000000,
                -10, -1000000, -1000000, -1000000, -1000000};
        int expectedPenaltiesWithoutLeft[] = new int[] {-10, -1000000, -1000000, -1000000, -10,
                -10, -1000000, -1000000, -1000000, -10};
        int expectedPenaltiesWithoutRight[] = new int[] {-10, -10, -1000000, -1000000, -1000000,
                -10, -10, -1000000, -1000000, -1000000};
        for (int i = -1; i <= 8; i++) {
            int expectedPenalty = expectedPenalties[i + 1];
            int expectedPenaltyWithoutLeft = expectedPenaltiesWithoutLeft[i + 1];
            int expectedPenaltyWithoutRight = expectedPenaltiesWithoutRight[i + 1];
            assertEquals(expectedPenalty, scoring.getGapPenalty(pattern, i,
                    true, true));
            assertEquals(expectedPenaltyWithoutLeft, scoring.getGapPenalty(pattern, i,
                    false, true));
            assertEquals(expectedPenaltyWithoutRight, scoring.getGapPenalty(pattern, i,
                    true, false));
        }
    }
}
