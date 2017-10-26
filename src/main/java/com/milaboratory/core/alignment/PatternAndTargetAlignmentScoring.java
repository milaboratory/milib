/*
 * Copyright 2017 MiLaboratory.com
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.milaboratory.core.sequence.NucleotideAlphabetCaseSensitive;
import com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive;

import java.io.ObjectStreamException;

import static com.milaboratory.core.sequence.SequenceQuality.BAD_QUALITY_VALUE;
import static com.milaboratory.core.sequence.SequenceQuality.GOOD_QUALITY_VALUE;

/**
 * Score system that uses penalty for gap, same as LinearGapAlignmentScoring, but with very high negative score
 * for gaps and insertions near uppercase letters. It disables gaps and insertions near uppercase letters.
 */
public final class PatternAndTargetAlignmentScoring extends AbstractAlignmentScoring<NucleotideSequenceCaseSensitive>
        implements java.io.Serializable  {
    /**
     * Penalty for gap near uppercase letter
     */
    private final int gapNearUppercasePenalty = -100000000;

    private final int matchScore;
    private final int mismatchScore;
    private final int gapPenalty;
    private final boolean usePatternLength;
    private final byte goodQuality;
    private final byte badQuality;
    private final int maxQualityPenalty;

    /**
     * Creates new PatternAndTargetAlignmentScoring. Required for deserialization defaults.
     */
    @SuppressWarnings("unchecked")
    private PatternAndTargetAlignmentScoring() {
        super(NucleotideSequenceCaseSensitive.ALPHABET, new SubstitutionMatrix(Integer.MIN_VALUE, Integer.MIN_VALUE));
        matchScore = Integer.MIN_VALUE;
        mismatchScore = Integer.MIN_VALUE;
        gapPenalty = Integer.MIN_VALUE;
        usePatternLength = false;
        goodQuality = GOOD_QUALITY_VALUE;
        badQuality = BAD_QUALITY_VALUE;
        maxQualityPenalty = 0;
    }

    @JsonCreator
    public PatternAndTargetAlignmentScoring(
            @JsonProperty("alphabet") NucleotideAlphabetCaseSensitive alphabet,
            @JsonProperty("gapNearUppercasePenalty") int gapNearUppercasePenalty,
            @JsonProperty("matchScore") int matchScore,
            @JsonProperty("mismatchScore") int mismatchScore,
            @JsonProperty("gapPenalty") int gapPenalty,
            @JsonProperty("usePatternLength") boolean usePatternLength,
            @JsonProperty("goodQuality") byte goodQuality,
            @JsonProperty("badQuality") byte badQuality,
            @JsonProperty("maxQualityPenalty") int maxQualityPenalty) {
        super(NucleotideSequenceCaseSensitive.ALPHABET, new SubstitutionMatrix(matchScore, mismatchScore));
        if ((matchScore > 0) || (mismatchScore >= 0) || (gapPenalty >= 0) || (maxQualityPenalty > 0))
            throw new IllegalArgumentException();
        this.matchScore = matchScore;
        this.mismatchScore = mismatchScore;
        this.gapPenalty = gapPenalty;
        this.usePatternLength = usePatternLength;
        this.goodQuality = goodQuality;
        this.badQuality = badQuality;
        this.maxQualityPenalty = maxQualityPenalty;
    }

    /**
     * Creates new PatternAndTargetAlignmentScoring.
     *
     * @param matchScore match score <= 0
     * @param mismatchScore mismatch score < 0
     * @param gapPenalty gap penalty < 0
     * @param usePatternLength true if we shall consider pattern length in score calculation
     * @param goodQuality this or better quality will not get score penalty
     * @param badQuality this or worse quality will get maximal score penalty
     * @param maxQualityPenalty score penalty value for badQuality or worse, <= 0
     */
    public PatternAndTargetAlignmentScoring(int matchScore, int mismatchScore, int gapPenalty, boolean usePatternLength,
                                            byte goodQuality, byte badQuality, int maxQualityPenalty) {
        super(NucleotideSequenceCaseSensitive.ALPHABET, new SubstitutionMatrix(matchScore, mismatchScore));
        if ((matchScore > 0) || (mismatchScore >= 0) || (gapPenalty >= 0) || (maxQualityPenalty > 0))
            throw new IllegalArgumentException();
        this.matchScore = matchScore;
        this.mismatchScore = mismatchScore;
        this.gapPenalty = gapPenalty;
        this.usePatternLength = usePatternLength;
        this.goodQuality = goodQuality;
        this.badQuality = badQuality;
        this.maxQualityPenalty = maxQualityPenalty;
    }

    /**
     * Get gap or insertion penalty by checking pattern subsequence: are there uppercase letters or not.
     *
     * @param pattern pattern case sensitive sequence
     * @param x coordinate of the matrix cell to which we want to write result, in the pattern; it may be -1
     * @return gap penalty
     */
    public int getGapPenalty(NucleotideSequenceCaseSensitive pattern, int x) {
        NucleotideAlphabetCaseSensitive alphabet = NucleotideSequenceCaseSensitive.ALPHABET;
        int left = Math.max(0, x - 1);
        int right = Math.min(pattern.size() - 1, x + 1);
        for (int i = left; i <= right; i++)
            if (Character.isUpperCase(alphabet.codeToSymbol(pattern.codeAt(i))))
                return gapNearUppercasePenalty;
        return gapPenalty;
    }

    /**
     * Get score for matched nucleotide using known target quality.
     *
     * @param from code of letter which is to be replaced
     * @param to code of letter which is replacing
     * @param quality quality for this matched nucleotide in target
     * @return match score that includes correction based on target quality
     */
    public int getScore(byte from, byte to, byte quality) {
        return getScore(from, to) + maxQualityPenalty * (Math.min(goodQuality, Math.max(badQuality, quality))
                - badQuality) / Math.max(1, goodQuality - badQuality);
    }

    /**
     * Update score for alignment using pattern length.
     *
     * @param rawAlignmentScore alignment score that doesn't include correction based on pattern length
     * @param patternLength pattern length
     * @return alignment score that includes correction based on pattern length
     */
    public int calculateAlignmentScore(int rawAlignmentScore, int patternLength) {
        if (usePatternLength)
            return (int)(rawAlignmentScore / (Math.log(patternLength + 1) / Math.log(2)));
        else
            return rawAlignmentScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PatternAndTargetAlignmentScoring that = (PatternAndTargetAlignmentScoring) o;

        return matchScore == that.matchScore && mismatchScore == that.mismatchScore && gapPenalty == that.gapPenalty
                && usePatternLength == that.usePatternLength && goodQuality == that.goodQuality
                && badQuality == that.badQuality && maxQualityPenalty == that.maxQualityPenalty;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + matchScore;
        result = 31 * result + mismatchScore;
        result = 31 * result + gapPenalty;
        result = 31 * result + (usePatternLength ? 1 : 0);
        result = 31 * result + (int) goodQuality;
        result = 31 * result + (int) badQuality;
        result = 31 * result + maxQualityPenalty;
        return result;
    }

    /* Internal methods for Java Serialization */

    protected Object writeReplace() throws ObjectStreamException {
        return new SerializationObject(gapNearUppercasePenalty, matchScore, mismatchScore, gapPenalty, usePatternLength,
                goodQuality, badQuality, maxQualityPenalty);
    }

    protected static class SerializationObject implements java.io.Serializable {
        final int gapNearUppercasePenalty;
        final int matchScore;
        final int mismatchScore;
        final int gapPenalty;
        final boolean usePatternLength;
        final byte goodQuality;
        final byte badQuality;
        final int maxQualityPenalty;

        public SerializationObject() {
            this(0, 0, 0, 0, false,
                    (byte)0, (byte)0, 0);
        }

        public SerializationObject(int gapNearUppercasePenalty, int matchScore, int mismatchScore, int gapPenalty,
                                   boolean usePatternLength, byte goodQuality, byte badQuality, int maxQualityPenalty) {
            this.gapNearUppercasePenalty = gapNearUppercasePenalty;
            this.matchScore = matchScore;
            this.mismatchScore = mismatchScore;
            this.gapPenalty = gapPenalty;
            this.usePatternLength = usePatternLength;
            this.goodQuality = goodQuality;
            this.badQuality = badQuality;
            this.maxQualityPenalty = maxQualityPenalty;
        }

        @SuppressWarnings("unchecked")
        private Object readResolve()
                throws ObjectStreamException {
            return new PatternAndTargetAlignmentScoring(NucleotideSequenceCaseSensitive.ALPHABET,
                    gapNearUppercasePenalty, matchScore, mismatchScore, gapPenalty, usePatternLength,
                    goodQuality, badQuality, maxQualityPenalty);
        }
    }
}
