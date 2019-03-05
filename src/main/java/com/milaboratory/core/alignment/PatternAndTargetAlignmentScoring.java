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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive;
import com.milaboratory.util.GlobalObjectMappers;

import java.io.ObjectStreamException;

import static com.milaboratory.core.sequence.SequenceQuality.BAD_QUALITY_VALUE;
import static com.milaboratory.core.sequence.SequenceQuality.GOOD_QUALITY_VALUE;

/**
 * Score system that uses penalty for gap, same as LinearGapAlignmentScoring, but with very high negative score
 * for gaps and insertions near uppercase letters. It disables gaps and insertions near uppercase letters.
 */
public final class PatternAndTargetAlignmentScoring extends AbstractAlignmentScoring<NucleotideSequenceCaseSensitive>
        implements java.io.Serializable  {
    /** Penalty for gap near uppercase letter */
    private static final int gapNearUppercasePenalty = -1000000;

    private final int matchScore;
    private final int mismatchScore;
    private final int gapPenalty;
    private final int uppercaseMismatchScore;
    private final byte goodQuality;
    private final byte badQuality;
    private final int maxQualityPenalty;
    private final boolean qualityPenaltyForN;

    /**
     * Creates new PatternAndTargetAlignmentScoring. Required for deserialization defaults.
     */
    private PatternAndTargetAlignmentScoring() {
        super(NucleotideSequenceCaseSensitive.ALPHABET, new SubstitutionMatrix(Integer.MIN_VALUE, Integer.MIN_VALUE));
        matchScore = Integer.MIN_VALUE;
        mismatchScore = Integer.MIN_VALUE;
        gapPenalty = Integer.MIN_VALUE;
        uppercaseMismatchScore = Integer.MIN_VALUE;
        goodQuality = GOOD_QUALITY_VALUE;
        badQuality = BAD_QUALITY_VALUE;
        maxQualityPenalty = 0;
        qualityPenaltyForN = false;
    }

    @JsonCreator
    public PatternAndTargetAlignmentScoring(
            @JsonProperty("alphabet") Alphabet alphabet,
            @JsonProperty("subsMatrix") SubstitutionMatrix subsMatrix,
            @JsonProperty("matchScore") int matchScore,
            @JsonProperty("mismatchScore") int mismatchScore,
            @JsonProperty("gapPenalty") int gapPenalty,
            @JsonProperty("uppercaseMismatchScore") int uppercaseMismatchScore,
            @JsonProperty("goodQuality") byte goodQuality,
            @JsonProperty("badQuality") byte badQuality,
            @JsonProperty("maxQualityPenalty") int maxQualityPenalty,
            @JsonProperty("qualityPenaltyForN") boolean qualityPenaltyForN) {
        super(NucleotideSequenceCaseSensitive.ALPHABET, subsMatrix);
        if ((matchScore > 0) || (mismatchScore >= 0) || (gapPenalty >= 0) || (maxQualityPenalty > 0))
            throw new IllegalArgumentException();
        this.matchScore = matchScore;
        this.mismatchScore = mismatchScore;
        this.gapPenalty = gapPenalty;
        this.uppercaseMismatchScore = uppercaseMismatchScore;
        this.goodQuality = goodQuality;
        this.badQuality = badQuality;
        this.maxQualityPenalty = maxQualityPenalty;
        this.qualityPenaltyForN = qualityPenaltyForN;
    }

    /**
     * Creates new PatternAndTargetAlignmentScoring.
     *
     * @param matchScore match score
     * @param mismatchScore mismatch score < 0
     * @param gapPenalty gap penalty < 0
     * @param uppercaseMismatchScore uppercase letters mismatch score < 0
     * @param goodQuality this or better quality will not get score penalty
     * @param badQuality this or worse quality will get maximal score penalty
     * @param maxQualityPenalty score penalty value for badQuality or worse, <= 0
     * @param qualityPenaltyForN true if we will use quality penalty for N letters in pattern, otherwise false
     */
    public PatternAndTargetAlignmentScoring(int matchScore, int mismatchScore, int gapPenalty,
                                            int uppercaseMismatchScore, byte goodQuality, byte badQuality,
                                            int maxQualityPenalty, boolean qualityPenaltyForN) {
        super(NucleotideSequenceCaseSensitive.ALPHABET, new SubstitutionMatrix(matchScore, mismatchScore));
        if ((mismatchScore >= Math.min(0, matchScore)) || (gapPenalty >= Math.min(0, matchScore))
                || (maxQualityPenalty > 0) || (uppercaseMismatchScore > mismatchScore))
            throw new IllegalArgumentException("matchScore=" + matchScore + ", mismatchScore=" + mismatchScore
                    + ", gapPenalty=" + gapPenalty + ", uppercaseMismatchScore=" + uppercaseMismatchScore
                    + ", maxQualityPenalty=" + maxQualityPenalty);
        this.matchScore = matchScore;
        this.mismatchScore = mismatchScore;
        this.gapPenalty = gapPenalty;
        this.uppercaseMismatchScore = uppercaseMismatchScore;
        this.goodQuality = goodQuality;
        this.badQuality = badQuality;
        this.maxQualityPenalty = maxQualityPenalty;
        this.qualityPenaltyForN = qualityPenaltyForN;
    }

    /**
     * Get gap or insertion penalty by checking pattern subsequence: are there uppercase letters or not.
     *
     * @param pattern pattern case sensitive sequence
     * @param x coordinate of the matrix cell to which we want to write result, in the pattern; it may be -1
     * @param checkLeftFromCurrent must be true to check if this pattern letter can be deleted, or false to check
     *                             if there can be inserted letter after this pattern letter
     * @param checkRightFromCurrent must be true to check if this pattern letter can be deleted, or false to check
     *                              if there can be inserted letter before this pattern letter
     *                              (for right-to-left alignment)
     * @return gap penalty
     */
    public int getGapPenalty(NucleotideSequenceCaseSensitive pattern, int x,
                             boolean checkLeftFromCurrent, boolean checkRightFromCurrent) {
        int left = Math.max(0, checkLeftFromCurrent ? x - 1 : x);
        int right = Math.min(pattern.size() - 1, checkRightFromCurrent ? x + 1 : x);
        for (int i = left; i <= right; i++)
            if (Character.isUpperCase(alphabet.codeToSymbol(pattern.codeAt(i))))
                return gapNearUppercasePenalty;
        return gapPenalty;
    }

    /**
     * Get score for matched nucleotide with extra check for uppercase mismatch penalty.
     *
     * @param from code of letter in pattern which is to be replaced
     * @param to code of letter in target which is replacing
     * @return match score with possible extra penalty for uppercase mismatch
     */
    @Override
    public int getScore(byte from, byte to) {
        int basicScore = super.getScore(from, to);
        // for case sensitive alphabet, possible results for basicScore are only matchScore and mismatchScore
        boolean isMismatch = (basicScore == mismatchScore);
        return (isMismatch && Character.isUpperCase(alphabet.codeToSymbol(from))) ? uppercaseMismatchScore : basicScore;
    }

    /**
     * Get score for matched nucleotide using known target quality.
     *
     * @param from code of letter in pattern which is to be replaced
     * @param to code of letter in target which is replacing
     * @param quality quality for this matched nucleotide in target
     * @return match score that includes correction based on target quality
     */
    public int getScore(byte from, byte to, byte quality) {
        int basicScore = getScore(from, to);
        int extraQualityPenalty = maxQualityPenalty * (goodQuality
                - Math.min(goodQuality, Math.max(badQuality, quality))) / Math.max(1, goodQuality - badQuality);
        return basicScore + extraQualityPenalty;
    }

    @Override
    public String toString() {
        try {
            return GlobalObjectMappers.PRETTY.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PatternAndTargetAlignmentScoring that = (PatternAndTargetAlignmentScoring)o;

        if (matchScore != that.matchScore) return false;
        if (mismatchScore != that.mismatchScore) return false;
        if (gapPenalty != that.gapPenalty) return false;
        if (uppercaseMismatchScore != that.uppercaseMismatchScore) return false;
        if (goodQuality != that.goodQuality) return false;
        if (badQuality != that.badQuality) return false;
        if (maxQualityPenalty != that.maxQualityPenalty) return false;
        return qualityPenaltyForN == that.qualityPenaltyForN;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + matchScore;
        result = 31 * result + mismatchScore;
        result = 31 * result + gapPenalty;
        result = 31 * result + uppercaseMismatchScore;
        result = 31 * result + (int)goodQuality;
        result = 31 * result + (int)badQuality;
        result = 31 * result + maxQualityPenalty;
        result = 31 * result + (qualityPenaltyForN ? 1 : 0);
        return result;
    }

    /* Internal methods for Java Serialization */

    protected Object writeReplace() throws ObjectStreamException {
        return new SerializationObject(alphabet, subsMatrix, matchScore, mismatchScore,
                gapPenalty, uppercaseMismatchScore, goodQuality, badQuality, maxQualityPenalty, qualityPenaltyForN);
    }

    protected static class SerializationObject implements java.io.Serializable {
        final Alphabet alphabet;
        final SubstitutionMatrix matrix;
        private final int matchScore;
        private final int mismatchScore;
        private final int gapPenalty;
        private final int uppercaseMismatchScore;
        private final byte goodQuality;
        private final byte badQuality;
        private final int maxQualityPenalty;
        private final boolean qualityPenaltyForN;

        public SerializationObject() {
            this(null, null, 0, 0, 0, 0,
                    (byte)0, (byte)0, 0, false);
        }

        public SerializationObject(
                Alphabet alphabet, SubstitutionMatrix matrix, int matchScore, int mismatchScore, int gapPenalty,
                int uppercaseMismatchScore, byte goodQuality, byte badQuality, int maxQualityPenalty,
                boolean qualityPenaltyForN) {
            this.alphabet = alphabet;
            this.matrix = matrix;
            this.matchScore = matchScore;
            this.mismatchScore = mismatchScore;
            this.gapPenalty = gapPenalty;
            this.uppercaseMismatchScore = uppercaseMismatchScore;
            this.goodQuality = goodQuality;
            this.badQuality = badQuality;
            this.maxQualityPenalty = maxQualityPenalty;
            this.qualityPenaltyForN = qualityPenaltyForN;
        }

        private Object readResolve() throws ObjectStreamException {
            return new PatternAndTargetAlignmentScoring(alphabet, matrix, matchScore, mismatchScore, gapPenalty,
                    uppercaseMismatchScore, goodQuality, badQuality, maxQualityPenalty, qualityPenaltyForN);
        }
    }
}
