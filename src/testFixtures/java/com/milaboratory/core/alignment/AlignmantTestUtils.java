package com.milaboratory.core.alignment;

import com.milaboratory.core.sequence.Sequence;
import org.junit.Assert;

public class AlignmantTestUtils {
    public static <S extends Sequence<S>> void assertAlignment(Alignment<S> alignment, S s2) {
        Assert.assertEquals(
                alignment.getRelativeMutations().mutate(alignment.sequence1.getRange(
                        alignment.getSequence1Range())), s2.getRange(alignment.getSequence2Range()));
    }

    public static <S extends Sequence<S>> void assertAlignment(Alignment<S> alignment, S s2, AlignmentScoring<S> scoring) {
        assertAlignment(alignment, s2);
        int calculatedScoring = alignment.calculateScore(scoring);
        if (calculatedScoring != alignment.getScore()) {
            System.out.println("Actual score: " + alignment.getScore());
            System.out.println("Expected score: " + calculatedScoring);
            System.out.println("Actual alignment: ");
            System.out.println(alignment);
            System.out.println();
        }
        Assert.assertEquals(calculatedScoring, alignment.getScore(), 0.1);
    }
}
