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

import com.milaboratory.core.Range;
import com.milaboratory.core.motif.BitapMatcher;
import com.milaboratory.core.motif.BitapPattern;
import com.milaboratory.core.mutations.MutationsBuilder;
import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.NucleotideSequence;

public final class BandedLinearAligner {
    private static final Range RANGE0 = new Range(0, 0);
    private static final Range RANGE1 = new Range(0, 1);
    private static final Alignment<NucleotideSequence> EMPTY_ALIGNMENT = new Alignment<>(NucleotideSequence.EMPTY,
            new MutationsBuilder<>(NucleotideSequence.ALPHABET).createAndDestroy(), RANGE0, RANGE0, 0);

    private BandedLinearAligner() {
    }

    /**
     * Classical Banded Alignment
     * <p/>
     * <p>Both sequences must be highly similar</p> <p>Align 2 sequence completely (i.e. while first sequence will be
     * aligned against whole second sequence)</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param offset1     offset in first sequence
     * @param length1     length of first sequence's part to be aligned
     * @param offset2     offset in second sequence
     * @param length2     length of second sequence's part to be aligned
     * @param width       width of banded alignment matrix. In other terms max allowed number of indels
     * @param mutations   mutations array where all mutations will be kept
     * @param cachedArray cached (created once) array to be used in {@link BandedMatrix}, which is compact alignment
     *                    scoring matrix
     */
    public static float align0(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                               int offset1, int length1, int offset2, int length2,
                               int width, MutationsBuilder<NucleotideSequence> mutations, CachedIntArray cachedArray) {
        if(offset1 < 0 || length1 < 0 || offset2 < 0 || length2 < 0)
            throw new IllegalArgumentException();

        int size1 = length1 + 1,
                size2 = length2 + 1;

        BandedMatrix matrix = new BandedMatrix(cachedArray, size1, size2, width);

        int i, j;

        for (i = matrix.getRowFactor() - matrix.getColumnDelta(); i > 0; --i)
            matrix.set(0, i, scoring.getGapPenalty() * i);

        for (i = matrix.getColumnDelta(); i > 0; --i)
            matrix.set(i, 0, scoring.getGapPenalty() * i);

        matrix.set(0, 0, 0);

        int match, delete, insert, to;

        for (i = 0; i < length1; ++i) {
            to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, length2);
            for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; ++j) {
                match = matrix.get(i, j) +
                        scoring.getScore(seq1.codeAt(offset1 + i), seq2.codeAt(offset2 + j));
                delete = matrix.get(i, j + 1) + scoring.getGapPenalty();
                insert = matrix.get(i + 1, j) + scoring.getGapPenalty();
                matrix.set(i + 1, j + 1, Math.max(match, Math.max(delete, insert)));
            }
        }

        to = mutations.size();
        i = length1 - 1;
        j = length2 - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i >= 0 && j >= 0 &&
                    matrix.get(i + 1, j + 1) == matrix.get(i, j) +
                            scoring.getScore(c1 = seq1.codeAt(offset1 + i),
                                    c2 = seq2.codeAt(offset2 + j))) {
                if (c1 != c2)
                    mutations.appendSubstitution(offset1 + i, c1, c2);
                --i;
                --j;
            } else if (i >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i, j + 1) + scoring.getGapPenalty()) {
                mutations.appendDeletion(offset1 + i, seq1.codeAt(offset1 + i));
                --i;
            } else if (j >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i + 1, j) + scoring.getGapPenalty()) {
                mutations.appendInsertion(offset1 + i + 1, seq2.codeAt(offset2 + j));
                --j;
            } else
                throw new RuntimeException();
        }

        mutations.reverseRange(to, mutations.size());
        return matrix.get(length1, length2);
    }

    /**
     * Semi-semi-global alignment with artificially added letters.
     * <p/>
     * <p>Alignment where second sequence is aligned to the right part of first sequence.</p> <p>Whole second sequence
     * must be highly similar to the first sequence</p>
     *
     * @param scoring           scoring system
     * @param seq1              first sequence
     * @param seq2              second sequence
     * @param offset1           offset in first sequence
     * @param length1           length of first sequence's part to be aligned including artificially added letters
     * @param addedNucleotides1 number of artificially added letters to the first sequence
     * @param offset2           offset in second sequence
     * @param length2           length of second sequence's part to be aligned including artificially added letters
     * @param addedNucleotides2 number of artificially added letters to the second sequence
     * @param width             width of banded alignment matrix. In other terms max allowed number of indels
     * @param mutations         mutations array where all mutations will be kept
     * @param cachedArray       cached (created once) array to be used in {@link BandedMatrix}, which is compact
     *                          alignment scoring matrix
     */
    public static BandedSemiLocalResult alignRightAdded0(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                         int offset1, int length1, int addedNucleotides1, int offset2, int length2, int addedNucleotides2,
                                                         int width, MutationsBuilder<NucleotideSequence> mutations, CachedIntArray cachedArray) {
        if(offset1 < 0 || length1 < 0 || offset2 < 0 || length2 < 0)
            throw new IllegalArgumentException();

        int size1 = length1 + 1,
                size2 = length2 + 1;

        BandedMatrix matrix = new BandedMatrix(cachedArray, size1, size2, width);

        int i, j;

        for (i = matrix.getRowFactor() - matrix.getColumnDelta(); i > 0; --i)
            matrix.set(0, i, scoring.getGapPenalty() * i);

        for (i = matrix.getColumnDelta(); i > 0; --i)
            matrix.set(i, 0, scoring.getGapPenalty() * i);

        matrix.set(0, 0, 0);

        int match, delete, insert, to;

        for (i = 0; i < length1; ++i) {
            to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, length2);
            for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; ++j) {
                match = matrix.get(i, j) +
                        scoring.getScore(seq1.codeAt(offset1 + i), seq2.codeAt(offset2 + j));
                delete = matrix.get(i, j + 1) + scoring.getGapPenalty();
                insert = matrix.get(i + 1, j) + scoring.getGapPenalty();
                matrix.set(i + 1, j + 1, Math.max(match, Math.max(delete, insert)));
            }
        }

        //Searching for max.
        int maxI = -1, maxJ = -1;
        int maxScore = Integer.MIN_VALUE;

        j = length2;
        for (i = length1 - addedNucleotides1; i < size1; ++i)
            if (maxScore < matrix.get(i, j)) {
                maxScore = matrix.get(i, j);
                maxI = i;
                maxJ = j;
            }

        i = length1;
        for (j = length2 - addedNucleotides2; j < size2; ++j)
            if (maxScore < matrix.get(i, j)) {
                maxScore = matrix.get(i, j);
                maxI = i;
                maxJ = j;
            }

        to = mutations.size();
        i = maxI - 1;
        j = maxJ - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i >= 0 && j >= 0 &&
                    matrix.get(i + 1, j + 1) == matrix.get(i, j) +
                            scoring.getScore(c1 = seq1.codeAt(offset1 + i),
                                    c2 = seq2.codeAt(offset2 + j))) {
                if (c1 != c2)
                    mutations.appendSubstitution(offset1 + i, c1, c2);
                --i;
                --j;
            } else if (i >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i, j + 1) + scoring.getGapPenalty()) {
                mutations.appendDeletion(offset1 + i, seq1.codeAt(offset1 + i));
                --i;
            } else if (j >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i + 1, j) + scoring.getGapPenalty()) {
                mutations.appendInsertion(offset1 + i + 1, seq2.codeAt(offset2 + j));
                --j;
            } else
                throw new RuntimeException();
        }

        mutations.reverseRange(to, mutations.size());

        return new BandedSemiLocalResult(offset1 + maxI - 1, offset2 + maxJ - 1, maxScore);
    }


    /**
     * Semi-semi-global alignment with artificially added letters.
     *
     * <p>Alignment where second sequence is aligned to the left part of first sequence.</p>
     *
     * <p>Whole second sequence must be highly similar to the first sequence, except last {@code width} letters, which
     * are to be checked whether they can improve alignment or not.</p>
     *
     * @param scoring           scoring system
     * @param seq1              first sequence
     * @param seq2              second sequence
     * @param offset1           offset in first sequence
     * @param length1           length of first sequence's part to be aligned
     * @param addedNucleotides1 number of artificially added letters to the first sequence;
     *                          must be 0 if seq1 is a pattern to match and seq2 is target sequence
     *                          where we search the pattern
     * @param offset2           offset in second sequence
     * @param length2           length of second sequence's part to be aligned
     * @param addedNucleotides2 number of artificially added letters to the second sequence;
     *                          if seq2 is target sequence where we search the pattern, this parameter must be equal
     *                          to maximum allowed number of indels (same as width parameter)
     * @param width             width of banded alignment matrix. In other terms max allowed number of indels
     * @param mutations         mutations array where all mutations will be kept
     * @param cachedArray       cached (created once) array to be used in {@link BandedMatrix}, which is compact
     *                          alignment scoring matrix
     */
    public static BandedSemiLocalResult alignLeftAdded0(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                        int offset1, int length1, int addedNucleotides1, int offset2, int length2, int addedNucleotides2,
                                                        int width, MutationsBuilder<NucleotideSequence> mutations, CachedIntArray cachedArray) {
        if(offset1 < 0 || length1 < 0 || offset2 < 0 || length2 < 0)
            throw new IllegalArgumentException();

        int size1 = length1 + 1,
                size2 = length2 + 1;

        BandedMatrix matrix = new BandedMatrix(cachedArray, size1, size2, width);

        int i, j;

        for (i = matrix.getRowFactor() - matrix.getColumnDelta(); i > 0; --i)
            matrix.set(0, i, scoring.getGapPenalty() * i);

        for (i = matrix.getColumnDelta(); i > 0; --i)
            matrix.set(i, 0, scoring.getGapPenalty() * i);

        matrix.set(0, 0, 0);

        int match, delete, insert, to;

        for (i = 0; i < length1; ++i) {
            to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, length2);
            for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; ++j) {
                match = matrix.get(i, j) +
                        scoring.getScore(seq1.codeAt(offset1 + length1 - 1 - i),
                                seq2.codeAt(offset2 + length2 - 1 - j));
                delete = matrix.get(i, j + 1) + scoring.getGapPenalty();
                insert = matrix.get(i + 1, j) + scoring.getGapPenalty();
                matrix.set(i + 1, j + 1, Math.max(match, Math.max(delete, insert)));
            }
        }

        //Searching for max.
        int maxI = -1, maxJ = -1;
        int maxScore = Integer.MIN_VALUE;

        j = length2;
        for (i = length1 - addedNucleotides1; i < size1; ++i)
            if (maxScore < matrix.get(i, j)) {
                maxScore = matrix.get(i, j);
                maxI = i;
                maxJ = j;
            }

        i = length1;
        for (j = length2 - addedNucleotides2; j < size2; ++j)
            if (maxScore < matrix.get(i, j)) {
                maxScore = matrix.get(i, j);
                maxI = i;
                maxJ = j;
            }

        i = maxI - 1;
        j = maxJ - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i >= 0 && j >= 0 &&
                    matrix.get(i + 1, j + 1) == matrix.get(i, j) +
                            scoring.getScore(c1 = seq1.codeAt(offset1 + length1 - 1 - i),
                                    c2 = seq2.codeAt(offset2 + length2 - 1 - j))) {
                if (c1 != c2)
                    mutations.appendSubstitution(offset1 + length1 - 1 - i, c1, c2);
                --i;
                --j;
            } else if (i >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i, j + 1) + scoring.getGapPenalty()) {
                mutations.appendDeletion(offset1 + length1 - 1 - i, seq1.codeAt(offset1 + length1 - 1 - i));
                --i;
            } else if (j >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i + 1, j) + scoring.getGapPenalty()) {
                mutations.appendInsertion(offset1 + length1 - 1 - i, seq2.codeAt(offset2 + length2 - 1 - j));
                --j;
            } else
                throw new RuntimeException();
        }

        return new BandedSemiLocalResult(offset1 + length1 - maxI, offset2 + length2 - maxJ, maxScore);
    }


    /**
     * Alignment which identifies what is the highly similar part of the both sequences.
     * <p/>
     * <p>Alignment is done in the way that beginning of second sequences is aligned to beginning of first
     * sequence.</p>
     * <p/>
     * <p>Alignment terminates when score in banded alignment matrix reaches {@code stopPenalty} value.</p>
     * <p/>
     * <p>In other words, only left part of second sequence is to be aligned</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param offset1     offset in first sequence
     * @param length1     length of first sequence's part to be aligned
     * @param offset2     offset in second sequence
     * @param length2     length of second sequence's part to be aligned@param width
     * @param stopPenalty alignment score value in banded alignment matrix at which alignment terminates
     * @param mutations   array where all mutations will be kept
     * @param cachedArray cached (created once) array to be used in {@link BandedMatrix}, which is compact alignment
     *                    scoring matrix
     * @return object which contains positions at which alignment terminated and array of mutations
     */
    public static BandedSemiLocalResult alignSemiLocalLeft0(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                            int offset1, int length1, int offset2, int length2,
                                                            int width, int stopPenalty, MutationsBuilder<NucleotideSequence> mutations,
                                                            CachedIntArray cachedArray) {
        if(offset1 < 0 || length1 < 0 || offset2 < 0 || length2 < 0)
            throw new IllegalArgumentException();

        int size1 = length1 + 1,
                size2 = length2 + 1;

        int matchReward = scoring.getScore((byte) 0, (byte) 0);

        BandedMatrix matrix = new BandedMatrix(cachedArray, size1, size2, width);

        int i, j;

        for (i = matrix.getRowFactor() - matrix.getColumnDelta(); i > 0; --i)
            matrix.set(0, i, scoring.getGapPenalty() * i);

        for (i = matrix.getColumnDelta(); i > 0; --i)
            matrix.set(i, 0, scoring.getGapPenalty() * i);

        matrix.set(0, 0, 0);

        int match, delete, insert, to;
        int max = 0;
        int iStop = 0, jStop = 0;
        int rowMax;

        for (i = 0; i < length1; ++i) {
            to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, size2 - 1);
            rowMax = Integer.MIN_VALUE;
            for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; ++j) {
                match = matrix.get(i, j) +
                        scoring.getScore(seq1.codeAt(offset1 + i), seq2.codeAt(offset2 + j));
                delete = matrix.get(i, j + 1) + scoring.getGapPenalty();
                insert = matrix.get(i + 1, j) + scoring.getGapPenalty();
                matrix.set(i + 1, j + 1, match = Math.max(match, Math.max(delete, insert)));
                if (max < match) {
                    iStop = i + 1;
                    jStop = j + 1;
                    max = match;
                }
                rowMax = Math.max(rowMax, match);
            }
            if (rowMax - i * matchReward < stopPenalty)
                break;
        }


        int fromL = mutations.size();

        i = iStop - 1;
        j = jStop - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i >= 0 && j >= 0 &&
                    matrix.get(i + 1, j + 1) == matrix.get(i, j) +
                            scoring.getScore(c1 = seq1.codeAt(offset1 + i),
                                    c2 = seq2.codeAt(offset2 + j))) {
                if (c1 != c2)
                    mutations.appendSubstitution(offset1 + i, c1, c2);
                --i;
                --j;
            } else if (i >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i, j + 1) + scoring.getGapPenalty()) {
                mutations.appendDeletion(offset1 + i, seq1.codeAt(offset1 + i));
                --i;
            } else if (j >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i + 1, j) + scoring.getGapPenalty()) {
                mutations.appendInsertion(offset1 + i + 1, seq2.codeAt(offset2 + j));
                --j;
            } else
                throw new RuntimeException();
        }

        mutations.reverseRange(fromL, mutations.size());

        return new BandedSemiLocalResult(offset1 + iStop - 1, offset2 + jStop - 1, max);
    }

    /**
     * Alignment which identifies what is the highly similar part of the both sequences.
     * <p/>
     * <p>Alignment is done in the way that end of second sequence is aligned to end of first sequence.</p> <p>Alignment
     * terminates when score in banded alignment matrix reaches {@code stopPenalty} value.</p> <p>In other words, only
     * right part of second sequence is to be aligned.</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param offset1     offset in first sequence
     * @param length1     length of first sequence's part to be aligned
     * @param offset2     offset in second sequence
     * @param length2     length of second sequence's part to be aligned@param width
     * @param stopPenalty alignment score value in banded alignment matrix at which alignment terminates
     * @param mutations   array where all mutations will be kept
     * @param cachedArray cached (created once) array to be used in {@link BandedMatrix}, which is compact alignment
     *                    scoring matrix
     * @return object which contains positions at which alignment terminated and array of mutations
     */
    public static BandedSemiLocalResult alignSemiLocalRight0(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                             int offset1, int length1, int offset2, int length2,
                                                             int width, int stopPenalty, MutationsBuilder<NucleotideSequence> mutations,
                                                             CachedIntArray cachedArray) {
        if(offset1 < 0 || length1 < 0 || offset2 < 0 || length2 < 0)
            throw new IllegalArgumentException();

        int size1 = length1 + 1,
                size2 = length2 + 1;

        int matchReward = scoring.getScore((byte) 0, (byte) 0);

        BandedMatrix matrix = new BandedMatrix(cachedArray, size1, size2, width);

        int i, j;

        for (i = matrix.getRowFactor() - matrix.getColumnDelta(); i > 0; --i)
            matrix.set(0, i, scoring.getGapPenalty() * i);

        for (i = matrix.getColumnDelta(); i > 0; --i)
            matrix.set(i, 0, scoring.getGapPenalty() * i);

        matrix.set(0, 0, 0);

        int match, delete, insert, to;
        int max = 0;
        int iStop = 0, jStop = 0;
        int rowMax;

        for (i = 0; i < length1; ++i) {
            to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, length2);
            rowMax = Integer.MIN_VALUE;
            for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; ++j) {
                match = matrix.get(i, j) +
                        scoring.getScore(seq1.codeAt(offset1 + length1 - 1 - i),
                                seq2.codeAt(offset2 + length2 - 1 - j));
                delete = matrix.get(i, j + 1) + scoring.getGapPenalty();
                insert = matrix.get(i + 1, j) + scoring.getGapPenalty();
                matrix.set(i + 1, j + 1, match = Math.max(match, Math.max(delete, insert)));
                if (max < match) {
                    iStop = i + 1;
                    jStop = j + 1;
                    max = match;
                }
                rowMax = Math.max(rowMax, match);
            }
            if (rowMax - i * matchReward < stopPenalty)
                break;
        }

        i = iStop - 1;
        j = jStop - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i >= 0 && j >= 0 &&
                    matrix.get(i + 1, j + 1) == matrix.get(i, j) +
                            scoring.getScore(c1 = seq1.codeAt(offset1 + length1 - 1 - i),
                                    c2 = seq2.codeAt(offset2 + length2 - 1 - j))) {
                if (c1 != c2)
                    mutations.appendSubstitution(offset1 + length1 - 1 - i, c1, c2);
                --i;
                --j;
            } else if (i >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i, j + 1) + scoring.getGapPenalty()) {
                mutations.appendDeletion(offset1 + length1 - 1 - i, seq1.codeAt(offset1 + length1 - 1 - i));
                --i;
            } else if (j >= 0 &&
                    matrix.get(i + 1, j + 1) ==
                            matrix.get(i + 1, j) + scoring.getGapPenalty()) {
                mutations.appendInsertion(offset1 + length1 - 1 - i, seq2.codeAt(offset2 + length2 - 1 - j));
                --j;
            } else
                throw new RuntimeException();
        }

        return new BandedSemiLocalResult(offset1 + length1 - iStop, offset2 + length2 - jStop, max);
    }


    /**
     * Classical Banded Alignment
     * <p/>
     * <p>Both sequences must be highly similar</p> <p>Align 2 sequence completely (i.e. while first sequence will be
     * aligned against whole second sequence)</p>
     *
     * @param scoring scoring system
     * @param seq1    first sequence
     * @param seq2    second sequence
     * @param width   width of banded alignment matrix. In other terms max allowed number of indels
     */
    public static Alignment<NucleotideSequence> align(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                      int width) {
        return align(scoring, seq1, seq2, 0, seq1.size(), 0, seq2.size(), width);
    }

    /**
     * Classical Banded Alignment
     * <p/>
     * <p>Both sequences must be highly similar</p> <p>Align 2 sequence completely (i.e. while first sequence will be
     * aligned against whole second sequence)</p>
     *
     * @param scoring scoring system
     * @param seq1    first sequence
     * @param seq2    second sequence
     * @param offset1 offset in first sequence
     * @param length1 length of first sequence's part to be aligned
     * @param offset2 offset in second sequence
     * @param length2 length of second sequence's part to be aligned
     * @param width   width of banded alignment matrix. In other terms max allowed number of indels
     */
    public static Alignment<NucleotideSequence> align(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                      int offset1, int length1, int offset2, int length2, int width) {
        try {
            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            float score = align0(scoring, seq1, seq2, offset1, length1, offset2, length2, width,
                    mutations, AlignmentCache.get());
            return new Alignment<>(seq1, mutations.createAndDestroy(),
                    new Range(offset1, offset1 + length1), new Range(offset2, offset2 + length2), score);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Semi-semi-global alignment with artificially added letters.
     *
     * <p>Alignment where second sequence is aligned to the left part of first sequence.</p>
     *
     * <p>Whole second sequence must be highly similar to the first sequence, except last {@code width} letters, which
     * are to be checked whether they can improve alignment or not.</p>
     *
     * @param scoring           scoring system
     * @param seq1              first sequence
     * @param seq2              second sequence
     * @param offset1           offset in first sequence
     * @param length1           length of first sequence's part to be aligned
     * @param addedNucleotides1 number of artificially added letters to the first sequence
     * @param offset2           offset in second sequence
     * @param length2           length of second sequence's part to be aligned
     * @param addedNucleotides2 number of artificially added letters to the second sequence
     * @param width             width of banded alignment matrix. In other terms max allowed number of indels
     */
    public static Alignment<NucleotideSequence> alignLeftAdded(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                               int offset1, int length1, int addedNucleotides1, int offset2, int length2, int addedNucleotides2,
                                                               int width) {
        try {
            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            BandedSemiLocalResult result = alignLeftAdded0(scoring, seq1, seq2,
                    offset1, length1, addedNucleotides1, offset2, length2, addedNucleotides2,
                    width, mutations, AlignmentCache.get());
            return new Alignment<>(seq1, mutations.createAndDestroy(),
                    new Range(result.sequence1Stop, offset1 + length1), new Range(result.sequence2Stop, offset2 + length2),
                    result.score);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Semi-semi-global alignment with artificially added letters.
     * <p/>
     * <p>Alignment where second sequence is aligned to the right part of first sequence.</p> <p>Whole second sequence
     * must be highly similar to the first sequence</p>
     *
     * @param scoring           scoring system
     * @param seq1              first sequence
     * @param seq2              second sequence
     * @param offset1           offset in first sequence
     * @param length1           length of first sequence's part to be aligned including artificially added letters
     * @param addedNucleotides1 number of artificially added letters to the first sequence
     * @param offset2           offset in second sequence
     * @param length2           length of second sequence's part to be aligned including artificially added letters
     * @param addedNucleotides2 number of artificially added letters to the second sequence
     * @param width             width of banded alignment matrix. In other terms max allowed number of indels
     */
    public static Alignment<NucleotideSequence> alignRightAdded(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                                int offset1, int length1, int addedNucleotides1, int offset2, int length2, int addedNucleotides2,
                                                                int width) {
        try {
            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            BandedSemiLocalResult result = alignRightAdded0(scoring, seq1, seq2,
                    offset1, length1, addedNucleotides1, offset2, length2, addedNucleotides2,
                    width, mutations, AlignmentCache.get());
            return new Alignment<>(seq1, mutations.createAndDestroy(),
                    new Range(offset1, result.sequence1Stop + 1), new Range(offset2, result.sequence2Stop + 1),
                    result.score);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Alignment which identifies what is the highly similar part of the both sequences.
     * <p/>
     * <p>Alignment is done in the way that beginning of second sequences is aligned to beginning of first
     * sequence.</p>
     * <p/>
     * <p>Alignment terminates when score in banded alignment matrix reaches {@code stopPenalty} value.</p>
     * <p/>
     * <p>In other words, only left part of second sequence is to be aligned</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param offset1     offset in first sequence
     * @param length1     length of first sequence's part to be aligned
     * @param offset2     offset in second sequence
     * @param length2     length of second sequence's part to be aligned@param width
     * @param stopPenalty alignment score value in banded alignment matrix at which alignment terminates
     * @return object which contains positions at which alignment terminated and array of mutations
     */
    public static Alignment<NucleotideSequence> alignSemiLocalLeft(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                                   int offset1, int length1, int offset2, int length2,
                                                                   int width, int stopPenalty) {
        try {
            int minLength = Math.min(length1, length2) + width + 1;
            length1 = Math.min(length1, minLength);
            length2 = Math.min(length2, minLength);
            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            BandedSemiLocalResult result = alignSemiLocalLeft0(scoring, seq1, seq2,
                    offset1, length1, offset2, length2, width, stopPenalty, mutations, AlignmentCache.get());
            return new Alignment<>(seq1, mutations.createAndDestroy(),
                    new Range(offset1, result.sequence1Stop + 1), new Range(offset2, result.sequence2Stop + 1),
                    result.score);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Alignment which identifies what is the highly similar part of the both sequences.
     * <p/>
     * <p>Alignment is done in the way that beginning of second sequences is aligned to beginning of first
     * sequence.</p>
     * <p/>
     * <p>Alignment terminates when score in banded alignment matrix reaches {@code stopPenalty} value.</p>
     * <p/>
     * <p>In other words, only left part of second sequence is to be aligned</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param stopPenalty alignment score value in banded alignment matrix at which alignment terminates
     * @return object which contains positions at which alignment terminated and array of mutations
     */
    public static Alignment<NucleotideSequence> alignSemiLocalLeft(LinearGapAlignmentScoring scoring,
                                                                   NucleotideSequence seq1, NucleotideSequence seq2,
                                                                   int width, int stopPenalty) {
        return alignSemiLocalLeft(scoring, seq1, seq2, 0, seq1.size(), 0, seq2.size(), width, stopPenalty);
    }

    /**
     * Alignment which identifies what is the highly similar part of the both sequences.
     * <p/>
     * <p>Alignment is done in the way that end of second sequence is aligned to end of first sequence.</p> <p>Alignment
     * terminates when score in banded alignment matrix reaches {@code stopPenalty} value.</p> <p>In other words, only
     * right part of second sequence is to be aligned.</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param offset1     offset in first sequence
     * @param length1     length of first sequence's part to be aligned
     * @param offset2     offset in second sequence
     * @param length2     length of second sequence's part to be aligned@param width
     * @param stopPenalty alignment score value in banded alignment matrix at which alignment terminates
     * @return object which contains positions at which alignment terminated and array of mutations
     */
    public static Alignment<NucleotideSequence> alignSemiLocalRight(LinearGapAlignmentScoring scoring, NucleotideSequence seq1, NucleotideSequence seq2,
                                                                    int offset1, int length1, int offset2, int length2,
                                                                    int width, int stopPenalty) {
        try {
            int minLength = Math.min(length1, length2) + width + 1;
            int l1 = Math.min(length1, minLength);
            int l2 = Math.min(length2, minLength);
            offset1 = offset1 + length1 - l1;
            offset2 = offset2 + length2 - l2;
            length1 = l1;
            length2 = l2;
            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            BandedSemiLocalResult result = alignSemiLocalRight0(scoring, seq1, seq2,
                    offset1, length1, offset2, length2, width,
                    stopPenalty, mutations, AlignmentCache.get());
            return new Alignment<>(seq1, mutations.createAndDestroy(),
                    new Range(result.sequence1Stop, offset1 + length1), new Range(result.sequence2Stop, offset2 + length2),
                    result.score);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Alignment which identifies what is the highly similar part of the both sequences.
     * <p/>
     * <p>Alignment is done in the way that end of second sequence is aligned to end of first sequence.</p> <p>Alignment
     * terminates when score in banded alignment matrix reaches {@code stopPenalty} value.</p> <p>In other words, only
     * right part of second sequence is to be aligned.</p>
     *
     * @param scoring     scoring system
     * @param seq1        first sequence
     * @param seq2        second sequence
     * @param stopPenalty alignment score value in banded alignment matrix at which alignment terminates
     * @return object which contains positions at which alignment terminated and array of mutations
     */
    public static Alignment<NucleotideSequence> alignSemiLocalRight(LinearGapAlignmentScoring scoring,
                                                                    NucleotideSequence seq1, NucleotideSequence seq2,
                                                                    int width, int stopPenalty) {
        return alignSemiLocalRight(scoring, seq1, seq2, 0, seq1.size(), 0, seq2.size(), width, stopPenalty);
    }

    /**
     * Banded alignment where shorter sequence is aligned globally (start and end must be in the alignment) and longer
     * sequence is aligned locally (not aligned tails are not penalized). For sequences of equal length, seq1 is
     * considered shorter.
     *
     * @param scoring   scoring system
     * @param seq1      first sequence with quality
     * @param seq2      second sequence with quality
     * @param width     width of banded alignment matrix. In other terms max allowed number of indels
     * @return          alignment object which contains mutations and aligned ranges in sequences
     */
    public static Alignment<NucleotideSequence> alignLocalGlobal(LinearGapAlignmentScoring scoring,
                                                                 NSequenceWithQuality seq1, NSequenceWithQuality seq2,
                                                                 int width) {
        if ((seq1.size() <= 1) && (seq2.size() <= 1))
            return alignLocalGlobalQuick(scoring, seq1.getSequence(),
                    (seq1.size() == 0) ? -1 : seq1.getSequence().codeAt(0),
                    (seq2.size() == 0) ? -1 : seq2.getSequence().codeAt(0));

        boolean seq1IsShorter = seq1.size() <= seq2.size();
        Range bestRange = findBestRange(seq1IsShorter ? seq1 : seq2, seq1IsShorter ? seq2 : seq1, width,
                scoring.getMaximalMatchScore() > 0);
        NucleotideSequence currentSeq1 = seq1IsShorter ? seq1.getSequence() : seq1.getSequence().getRange(bestRange);
        NucleotideSequence currentSeq2 = seq1IsShorter ? seq2.getSequence().getRange(bestRange) : seq2.getSequence();
        int size1 = currentSeq1.size() + 1;
        int size2 = currentSeq2.size() + 1;
        int mutOffset = seq1IsShorter ? 0 : bestRange.getLower();

        try {
            BandedMatrix matrix = new BandedMatrix(AlignmentCache.get(), size1, size2, width);
            int sizeI = matrix.getColumnDelta() + 1;
            int sizeJ = matrix.getRowFactor() - matrix.getColumnDelta() + 1;

            int i, j;
            matrix.set(0, 0, 0);
            for (i = 0; i < sizeI; i++)
                matrix.set(i, 0, getDefaultMatrixValue(scoring, seq1IsShorter, i, 0));
            for (j = 0; j < sizeJ; j++)
                matrix.set(0, j, getDefaultMatrixValue(scoring, seq1IsShorter, 0, j));

            int match, delete, insert;

            for (i = 0; i < currentSeq1.size(); i++) {
                for (j = Math.max(0, i - (sizeI - 1)); j < Math.min(i + sizeJ, currentSeq2.size()); j++) {
                    match = matrix.get(i, j) + scoring.getScore(currentSeq1.codeAt(i), currentSeq2.codeAt(j));
                    delete = matrix.get(i, j + 1) + scoring.getGapPenalty();
                    insert = matrix.get(i + 1, j) + scoring.getGapPenalty();
                    matrix.set(i + 1, j + 1, Math.max(match, Math.max(delete, insert)));
                }
            }

            int k, maxScore;
            int seq1RangeLower, seq1RangeUpper, seq2RangeLower, seq2RangeUpper;
            if (seq1IsShorter) {
                i = currentSeq1.size();
                seq1RangeLower = 0;
                seq1RangeUpper = i;
                maxScore = Integer.MIN_VALUE;
                for (k = Math.max(0, i - (sizeI - 1)); k <= currentSeq2.size(); k++) {
                    int currentScore = matrix.get(i, k);
                    if (currentScore > maxScore) {
                        j = k;
                        maxScore = currentScore;
                    }
                }
                seq2RangeLower = -1;
                seq2RangeUpper = j + bestRange.getLower();
            } else {
                j = currentSeq2.size();
                seq2RangeLower = 0;
                seq2RangeUpper = j;
                maxScore = Integer.MIN_VALUE;
                for (k = Math.max(0, j - (sizeJ - 1)); k <= currentSeq1.size(); k++) {
                    int currentScore = matrix.get(k, j);
                    if (currentScore > maxScore) {
                        i = k;
                        maxScore = currentScore;
                    }
                }
                seq1RangeLower = -1;
                seq1RangeUpper = i + bestRange.getLower();
            }

            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            byte c1, c2;
            while ((seq1IsShorter && i > 0) || (!seq1IsShorter && j > 0)) {
                if (i >= 1 && j >= 1 &&
                        matrix.get(i, j) == matrix.get(i - 1, j - 1) +
                                scoring.getScore(c1 = currentSeq1.codeAt(i - 1),
                                        c2 = currentSeq2.codeAt(j - 1))) {
                    if (c1 != c2)
                        mutations.appendSubstitution(mutOffset + i - 1, c1, c2);
                    --i;
                    --j;
                } else if (i >= 1 &&
                        matrix.get(i, j) == matrix.get(i - 1, j) + scoring.getGapPenalty()) {
                    mutations.appendDeletion(mutOffset + i - 1, currentSeq1.codeAt(i - 1));
                    --i;
                } else if (j >= 1 &&
                        matrix.get(i, j) ==
                                matrix.get(i, j - 1) + scoring.getGapPenalty()) {
                    mutations.appendInsertion(mutOffset + i, currentSeq2.codeAt(j - 1));
                    --j;
                } else
                    throw new RuntimeException();
            }

            if (seq1IsShorter)
                seq2RangeLower = j - i + bestRange.getLower();
            else
                seq1RangeLower = i - j + bestRange.getLower();
            mutations.reverseRange(0, mutations.size());
            return new Alignment<>(seq1.getSequence(), mutations.createAndDestroy(),
                    new Range(seq1RangeLower, seq1RangeUpper), new Range(seq2RangeLower, seq2RangeUpper), maxScore);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Quick alignment for alignLocalGlobal, for cases when both sequences have length 0 or 1.
     *
     * @param scoring       scoring system
     * @param seq1          first sequence, used in generated alignment
     * @param c1            code of character from first sequence, or -1 if first sequence have length 0
     * @param c2            code of character from second sequence, or -1 if second sequence have length 0
     * @return              alignment object which contains mutations and aligned ranges in sequences
     */
    private static Alignment<NucleotideSequence> alignLocalGlobalQuick(LinearGapAlignmentScoring scoring,
                                                                       NucleotideSequence seq1, byte c1, byte c2) {
        if ((c1 == -1) && (c2 == -1))
            return EMPTY_ALIGNMENT;
        else {
            MutationsBuilder<NucleotideSequence> mutations = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            if ((c1 != -1) && (c2 != -1)) {
                if (c1 != c2)
                    mutations.appendSubstitution(0, c1, c2);
                return new Alignment<>(seq1, mutations.createAndDestroy(), RANGE1, RANGE1, scoring.getScore(c1, c2));
            } else if (c1 == -1) {
                mutations.appendInsertion(0, c2);
                return new Alignment<>(NucleotideSequence.EMPTY, mutations.createAndDestroy(), RANGE0, RANGE1,
                        scoring.getGapPenalty());
            } else {
                mutations.appendDeletion(0, c1);
                return new Alignment<>(seq1, mutations.createAndDestroy(), RANGE1, RANGE0, scoring.getGapPenalty());
            }
        }
    }

    /**
     * Default matrix value for alignLocalGlobal.
     *
     * @param scoring           scoring system
     * @param seq1IsShorter     true if seq1 is shorter than seq2 or they have equal length
     * @param i                 coordinate in seq1
     * @param j                 coordinate in seq2
     * @return                  default value for not calculated element of matrix
     */
    private static int getDefaultMatrixValue(LinearGapAlignmentScoring scoring, boolean seq1IsShorter, int i, int j) {
        int distanceFromShorterStart = seq1IsShorter ? i : j;
        return distanceFromShorterStart * scoring.getGapPenalty();
    }

    /**
     * Detect best range in long sequence for local-global aligner.
     *
     * @param shortSeq              short sequence with quality
     * @param longSeq               long sequence with quality
     * @param width                 width of banded alignment matrix: max allowed number of indels
     * @param matchScoreIsPositive  true if match score is positive in alignment scoring
     * @return                      best range in long sequence to align short sequence
     */
    private static Range findBestRange(NSequenceWithQuality shortSeq, NSequenceWithQuality longSeq, int width,
                                       boolean matchScoreIsPositive) {
        final int BITAP_MAX_LENGTH = 63;
        int estimatedMaxErrors = width * 2;
        int shortSeqLeftTail = 0;
        BitapPattern bitapPattern;
        if (shortSeq.size() > BITAP_MAX_LENGTH) {
            int currentSumQuality = 0;
            for (int i = 0; i < BITAP_MAX_LENGTH; i++)
                currentSumQuality += shortSeq.getQuality().value(i);
            int currentPosition = 0;
            int bestQuality = currentSumQuality;
            while (currentPosition + BITAP_MAX_LENGTH + 1 < shortSeq.size()) {
                currentSumQuality -= shortSeq.getQuality().value(currentPosition);
                currentPosition++;
                currentSumQuality += shortSeq.getQuality().value(currentPosition + BITAP_MAX_LENGTH);
                if (currentSumQuality > bestQuality) {
                    bestQuality = currentSumQuality;
                    shortSeqLeftTail = currentPosition;
                }
            }
            bitapPattern = shortSeq.getSequence().getRange(shortSeqLeftTail, shortSeqLeftTail + BITAP_MAX_LENGTH)
                    .toMotif().getBitapPattern();
        } else
            bitapPattern = shortSeq.getSequence().toMotif().getBitapPattern();
        BitapMatcher bitapMatcher = bitapPattern.substitutionAndIndelMatcherFirst(estimatedMaxErrors,
                longSeq.getSequence());
        int bitapPos = bitapMatcher.findNext();
        int startPos = (bitapPos == -1) ? -1 : Math.max(0, bitapPos - shortSeqLeftTail);
        int minNumberOfErrors = Integer.MAX_VALUE;
        while (bitapPos != -1) {
            if (bitapMatcher.getNumberOfErrors() < minNumberOfErrors) {
                minNumberOfErrors = bitapMatcher.getNumberOfErrors();
                startPos = Math.max(0, bitapPos - shortSeqLeftTail);
            }
            bitapPos = bitapMatcher.findNext();
        }
        if (startPos != -1) {
            int currentSeqWidth = shortSeq.size() + (matchScoreIsPositive ? width
                    : Math.min(width, minNumberOfErrors));
            int endPos = Math.min(longSeq.size(), startPos + currentSeqWidth);
            startPos = Math.max(0, endPos - currentSeqWidth);
            return new Range(startPos, endPos);
        } else
            return new Range(0, longSeq.size());
    }
}
