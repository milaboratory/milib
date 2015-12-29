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
import com.milaboratory.core.mutations.MutationsBuilder;
import com.milaboratory.core.sequence.Sequence;

public final class Aligner {
    private Aligner() {
    }

    public static <S extends Sequence<S>> int alignOnlySubstitutions0(S seq1, S seq2, int seq1From, int seq1Length,
                                                                      int seq2From, int seq2Length,
                                                                      AlignmentScoring<S> scoring,
                                                                      MutationsBuilder<S> builder) {
        if (seq1Length != seq2Length)
            throw new IllegalArgumentException("Size of 'seq1' and 'seq2' sequences must be the same.");
        int score = 0;
        byte c1, c2;
        for (int i = 0; i < seq1Length; ++i) {
            if ((c1 = seq1.codeAt(seq1From + i)) != (c2 = seq2.codeAt(seq2From + i)))
                builder.appendSubstitution(seq1From + i, c1, c2);
            score += scoring.getScore(c1, c2);
        }
        return score;
    }

    public static <S extends Sequence<S>> Alignment<S> alignOnlySubstitutions(S seq1, S seq2,
                                                                              int seq1From, int seq1Length,
                                                                              int seq2From, int seq2Length,
                                                                              AlignmentScoring<S> scoring) {
        MutationsBuilder<S> builder = new MutationsBuilder<>(seq1.getAlphabet());
        int score = alignOnlySubstitutions0(seq1, seq2, seq1From, seq1Length, seq2From, seq2Length, scoring, builder);
        return new Alignment<>(seq1, builder.createAndDestroy(), new Range(seq1From, seq1From + seq1Length),
                new Range(seq2From, seq2From + seq2Length), score);
    }

    public static <S extends Sequence<S>> Alignment<S> alignOnlySubstitutions(S from, S to) {
        if (from.getAlphabet() != to.getAlphabet())
            throw new IllegalArgumentException();
        if (from.size() != to.size())
            throw new IllegalArgumentException("Size of 'from' and 'to' sequences must be the same.");
        MutationsBuilder<S> builder = new MutationsBuilder<S>(from.getAlphabet());
        int score = 0;
        for (int i = 0; i < from.size(); ++i)
            if (from.codeAt(i) != to.codeAt(i)) {
                builder.appendSubstitution(i, from.codeAt(i), to.codeAt(i));
                --score;
            } else ++score;
        Range range = new Range(0, from.size());
        return new Alignment<>(from, builder.createAndDestroy(), range, range, score);
    }

    /**
     * Performs global alignment
     *
     * @param alignmentScoring scoring system
     * @param seq1             first sequence
     * @param seq2             second sequence
     * @return array of mutations
     */
    public static <S extends Sequence<S>> Alignment<S> alignGlobal(AlignmentScoring<S> alignmentScoring,
                                                                   S seq1, S seq2) {
        if (alignmentScoring instanceof AffineGapAlignmentScoring)
            return alignGlobalAffine((AffineGapAlignmentScoring<S>) alignmentScoring, seq1, seq2);
        if (alignmentScoring instanceof LinearGapAlignmentScoring)
            return alignGlobalLinear((LinearGapAlignmentScoring<S>) alignmentScoring, seq1, seq2);
        throw new RuntimeException("Unknown scoring type.");
    }

    /**
     * Performs global alignment using Linear scoring system (penalty exists only for gap)
     *
     * @param scoring linear scoring system
     * @param seq1    first sequence
     * @param seq2    second sequence
     * @return array of mutations
     */
    public static <S extends Sequence<S>> Alignment<S> alignGlobalLinear(LinearGapAlignmentScoring scoring,
                                                                         S seq1, S seq2) {
        if (seq1.getAlphabet() != seq2.getAlphabet() ||
                seq1.getAlphabet() != scoring.getAlphabet())
            throw new IllegalArgumentException("Different alphabets.");

        int size1 = seq1.size() + 1,
                size2 = seq2.size() + 1;
        int[] matrix = new int[size1 * (seq2.size() + 1)];

        for (int i = 0; i < size2; ++i)
            matrix[i] = scoring.getGapPenalty() * i;

        for (int j = 1; j < size1; ++j)
            matrix[size2 * j] = scoring.getGapPenalty() * j;

        int i1, i2,
                match, delete, insert;

        for (i1 = 0; i1 < seq1.size(); ++i1)
            for (i2 = 0; i2 < seq2.size(); ++i2) {
                match = matrix[i1 * size2 + i2] +
                        scoring.getScore(seq1.codeAt(i1), seq2.codeAt(i2));
                delete = matrix[i1 * size2 + i2 + 1] + scoring.getGapPenalty();
                insert = matrix[(i1 + 1) * size2 + i2] + scoring.getGapPenalty();
                matrix[(i1 + 1) * size2 + i2 + 1] = Math.max(match, Math.max(delete, insert));
            }

        MutationsBuilder<S> builder = new MutationsBuilder<>(seq1.getAlphabet(), true);

        i1 = seq1.size() - 1;
        i2 = seq2.size() - 1;
        int score = matrix[(i1 + 1) * size2 + i2 + 1];

        while (i1 >= 0 || i2 >= 0) {
            if (i1 >= 0 && i2 >= 0 &&
                    matrix[(i1 + 1) * size2 + i2 + 1] == matrix[i1 * size2 + i2] +
                            scoring.getScore(seq1.codeAt(i1), seq2.codeAt(i2))) {
                if (seq1.codeAt(i1) != seq2.codeAt(i2))
                    builder.appendSubstitution(i1, seq1.codeAt(i1), seq2.codeAt(i2));
                --i1;
                --i2;
            } else if (i1 >= 0 &&
                    matrix[(i1 + 1) * size2 + i2 + 1] ==
                            matrix[i1 * size2 + i2 + 1] + scoring.getGapPenalty()) {
                builder.appendDeletion(i1, seq1.codeAt(i1));
                i1--;
            } else if (i2 >= 0 &&
                    matrix[(i1 + 1) * size2 + i2 + 1] ==
                            matrix[(i1 + 1) * size2 + i2] + scoring.getGapPenalty()) {
                builder.appendInsertion(i1 + 1, seq2.codeAt(i2));
                i2--;
            } else
                throw new RuntimeException();
        }

        return new Alignment<>(seq1, builder.createAndDestroy(), new Range(0, seq1.size()), new Range(0, seq2.size()),
                score);
    }

    /**
     * Performs global alignment using affine gap scoring system (different penalties exist for gap opening and gap
     * extension)
     *
     * @param scoring affine gap scoring system
     * @param seq1    first sequence
     * @param seq2    second sequence
     * @return array of mutations
     */
    public static <S extends Sequence<S>> Alignment<S> alignGlobalAffine(AffineGapAlignmentScoring<S> scoring,
                                                                         S seq1, S seq2) {
        if (seq1.getAlphabet() != seq2.getAlphabet() || seq1.getAlphabet() != scoring.getAlphabet())
            throw new IllegalArgumentException("Different alphabets.");

        int size1 = seq1.size() + 1,
                size2 = seq2.size() + 1;

        int[] alignXToGapAfterY = new int[size1 * size2];
        int[] alignYTOGapAfterX = new int[size1 * size2];
        int[] matrix = new int[size1 * size2];

        for (int j = 0; j < size2; ++j)
            matrix[j] = scoring.getAffineGapPenalty(j);

        for (int i = 0; i < size1; ++i)
            matrix[i * size2] = scoring.getAffineGapPenalty(i);

        matrix[0] = 0;

        for (int i = 0; i < size2; i++)
            alignXToGapAfterY[i] = -10000;

        for (int i = 0; i < size1; i++)
            alignYTOGapAfterX[i * size2] = -10000;

        for (int i = 1; i < size1; ++i) {
            for (int j = 1; j < size2; ++j) {
                int match = matrix[(i - 1) * size2 + j - 1] + scoring.getScore(seq1.codeAt(i - 1), seq2.codeAt(j - 1));

                alignXToGapAfterY[i * size2 + j] = Math.max(matrix[(i - 1) * size2 + j] + scoring.getGapOpenPenalty(), alignXToGapAfterY[(i - 1) * size2 + j] + scoring.getGapExtensionPenalty());
                alignYTOGapAfterX[i * size2 + j] = Math.max(matrix[i * size2 + j - 1] + scoring.getGapOpenPenalty(), alignYTOGapAfterX[i * size2 + j - 1] + scoring.getGapExtensionPenalty());

                matrix[i * size2 + j] = Math.max(match, Math.max(alignXToGapAfterY[i * size2 + j], alignYTOGapAfterX[i * size2 + j]));
            }
        }

        int i1 = seq1.size() - 1;
        int i2 = seq2.size() - 1;
        int v = matrix[(i1 + 1) * size2 + i2 + 1];
        final int maxV = v;

        MutationsBuilder<S> builder = new MutationsBuilder<S>(seq1.getAlphabet(), true);

        while (i1 >= 0 || i2 >= 0) {

            if (i1 >= 0 && v == alignXToGapAfterY[(i1 + 1) * size2 + i2 + 1]) {

                if (v == alignXToGapAfterY[i1 * size2 + i2 + 1] + scoring.getGapExtensionPenalty())
                    v = alignXToGapAfterY[i1 * size2 + i2 + 1];
                else
                    v = matrix[i1 * size2 + i2 + 1];

                builder.appendDeletion(i1, seq1.codeAt(i1));
                i1--;
            } else if (i2 >= 0 &&
                    v == alignYTOGapAfterX[(i1 + 1) * size2 + i2 + 1]) {

                if (v == alignYTOGapAfterX[(i1 + 1) * size2 + i2] + scoring.getGapExtensionPenalty())
                    v = alignYTOGapAfterX[(i1 + 1) * size2 + i2];
                else
                    v = matrix[(i1 + 1) * size2 + i2];

                builder.appendInsertion(i1 + 1, seq2.codeAt(i2));
                i2--;

            } else if (i1 >= 0 && i2 >= 0 && v == matrix[i1 * size2 + i2] + scoring.getScore(seq1.codeAt(i1), seq2.codeAt(i2))) {
                v = matrix[i1 * size2 + i2];

                if (seq1.codeAt(i1) != seq2.codeAt(i2))
                    builder.appendSubstitution(i1, seq1.codeAt(i1), seq2.codeAt(i2));

                --i1;
                --i2;
            }

            //gap up to first letter
            else if (i1 == -1) {
                builder.appendInsertion(i1 + 1, seq2.codeAt(i2));
                i2--;
            } else if (i2 == -1) {
                builder.appendDeletion(i1, seq1.codeAt(i1));
                i1--;
            } else
                throw new RuntimeException();
        }

        return new Alignment<S>(seq1, builder.createAndDestroy(),
                new Range(0, seq1.size()), new Range(0, seq2.size()), maxV);
    }

    /**
     * Performs local alignment
     *
     * @param alignmentScoring scoring system
     * @param seq1             first sequence
     * @param seq2             second sequence
     * @return result of alignment with information about alignment positions in both sequences and array of mutations
     */
    public static <S extends Sequence<S>> Alignment<S> alignLocal(AlignmentScoring<S> alignmentScoring,
                                                                  S seq1, S seq2) {
        if (alignmentScoring instanceof AffineGapAlignmentScoring)
            return alignLocalAffine((AffineGapAlignmentScoring<S>) alignmentScoring, seq1, seq2);
        if (alignmentScoring instanceof LinearGapAlignmentScoring)
            return alignLocalLinear((LinearGapAlignmentScoring<S>) alignmentScoring, seq1, seq2);
        throw new RuntimeException("Unknown scoring type.");
    }

    /**
     * Performs local alignment using Linear scoring system (penalty exists only for gap)
     *
     * @param seq1 first sequence
     * @param seq2 second sequence
     * @return result of alignment with information about alignment positions in both sequences and array of mutations
     */
    public static <S extends Sequence<S>> Alignment<S> alignLocalLinear(LinearGapAlignmentScoring<S> scoring,
                                                                        S seq1, S seq2) {
        if (seq1.getAlphabet() != seq2.getAlphabet() || seq1.getAlphabet() != scoring.getAlphabet())
            throw new IllegalArgumentException("Different alphabets.");

        int size1 = seq1.size() + 1,
                size2 = seq2.size() + 1;
        int[] matrix = new int[size1 * (seq2.size() + 1)];

        int i1, i2,
                match, delete, insert;

        int max = -1;
        int i1Start = 0;
        int i2Start = 0;

        for (i1 = 0; i1 < seq1.size(); ++i1)
            for (i2 = 0; i2 < seq2.size(); ++i2) {
                match = matrix[i1 * size2 + i2] +
                        scoring.getScore(seq1.codeAt(i1), seq2.codeAt(i2));
                delete = matrix[i1 * size2 + i2 + 1] + scoring.getGapPenalty();
                insert = matrix[(i1 + 1) * size2 + i2] + scoring.getGapPenalty();
                matrix[(i1 + 1) * size2 + i2 + 1] = Math.max(0, Math.max(match, Math.max(delete, insert)));

                if (matrix[(i1 + 1) * size2 + i2 + 1] > max && matrix[(i1 + 1) * size2 + i2 + 1] > 0) {
                    i1Start = i1 + 1;
                    i2Start = i2 + 1;
                    max = matrix[(i1 + 1) * size2 + i2 + 1];
                }
            }


        //it's not possible to find any local alignment
        if (max == -1)
            return null;

        MutationsBuilder<S> builder = new MutationsBuilder<S>(seq1.getAlphabet(), true);

        i1 = i1Start - 1;
        i2 = i2Start - 1;

        while (i1 >= 0 || i2 >= 0) {
            if (i1 >= 0 && i2 >= 0 &&
                    matrix[(i1 + 1) * size2 + i2 + 1] == matrix[i1 * size2 + i2] +
                            scoring.getScore(seq1.codeAt(i1), seq2.codeAt(i2))) {
                if (seq1.codeAt(i1) != seq2.codeAt(i2))
                    builder.appendSubstitution(i1, seq1.codeAt(i1), seq2.codeAt(i2));

                if (matrix[i1 * size2 + i2] == 0)
                    break;

                --i1;
                --i2;
            } else if (i1 >= 0 &&
                    matrix[(i1 + 1) * size2 + i2 + 1] ==
                            matrix[i1 * size2 + i2 + 1] + scoring.getGapPenalty()) {
                builder.appendDeletion(i1, seq1.codeAt(i1));

                if (matrix[i1 * size2 + i2 + 1] == 0)
                    break;

                i1--;
            } else if (i2 >= 0 &&
                    matrix[(i1 + 1) * size2 + i2 + 1] ==
                            matrix[(i1 + 1) * size2 + i2] + scoring.getGapPenalty()) {
                builder.appendInsertion(i1 + 1, seq2.codeAt(i2));

                if (matrix[(i1 + 1) * size2 + i2] == 0)
                    break;

                i2--;
            } else
                throw new RuntimeException();
        }

        int seq1Start = i1;
        int seq2Start = i2;

        return new Alignment<>(seq1, builder.createAndDestroy(),
                new Range(seq1Start, i1Start), new Range(seq2Start, i2Start), max);
    }

    /**
     * Performs local alignment using Affine gap scoring system (different penalties exist for gap opening and gap
     * extension)
     *
     * @param seq1 first sequence
     * @param seq2 second sequence
     * @return result of alignment with information about alignment positions in both sequences and array of mutations
     */
    public static <S extends Sequence<S>> Alignment<S> alignLocalAffine(AffineGapAlignmentScoring<S> scoring,
                                                                        S seq1, S seq2) {
        if (seq1.getAlphabet() != seq2.getAlphabet() || seq1.getAlphabet() != scoring.getAlphabet())
            throw new IllegalArgumentException("Different alphabets.");

        int size1 = seq1.size() + 1,
                size2 = seq2.size() + 1;

        int[] alignXToGapAfterY = new int[size1 * size2];
        int[] alignYTOGapAfterX = new int[size1 * size2];
        int[] matrix = new int[size1 * size2];

        for (int i = 0; i < size2; i++)
            alignXToGapAfterY[i] = -10000;

        for (int i = 0; i < size1; i++)
            alignYTOGapAfterX[i * size2] = -10000;

        int max = -1;
        int i1Start = 0;
        int i2Start = 0;

        for (int i = 1; i < size1; ++i) {
            for (int j = 1; j < size2; ++j) {
                int match = matrix[(i - 1) * size2 + j - 1] + scoring.getScore(seq1.codeAt(i - 1), seq2.codeAt(j - 1));

                alignXToGapAfterY[i * size2 + j] = Math.max(matrix[(i - 1) * size2 + j] + scoring.getGapOpenPenalty(), alignXToGapAfterY[(i - 1) * size2 + j] + scoring.getGapExtensionPenalty());
                alignYTOGapAfterX[i * size2 + j] = Math.max(matrix[i * size2 + j - 1] + scoring.getGapOpenPenalty(), alignYTOGapAfterX[i * size2 + j - 1] + scoring.getGapExtensionPenalty());

                matrix[i * size2 + j] = Math.max(0,
                        Math.max(match,
                                Math.max(alignXToGapAfterY[i * size2 + j],
                                        alignYTOGapAfterX[i * size2 + j]
                                )
                        )
                );

                if (matrix[i * size2 + j] > max && matrix[i * size2 + j] > 0) {
                    i1Start = i;
                    i2Start = j;
                    max = matrix[i * size2 + j];
                }
            }
        }


        //it's not possible to find any local alignment
        if (max == -1)
            return null;

        MutationsBuilder<S> builder = new MutationsBuilder<S>(seq1.getAlphabet(), true);

        int i1 = i1Start - 1;
        int i2 = i2Start - 1;
        int v = matrix[(i1 + 1) * size2 + i2 + 1];

        while (i1 >= 0 && i2 >= 0) {

            if (i1 >= 0 && v == alignXToGapAfterY[(i1 + 1) * size2 + i2 + 1]) {

                if (v == alignXToGapAfterY[i1 * size2 + i2 + 1] + scoring.getGapExtensionPenalty())
                    v = alignXToGapAfterY[i1 * size2 + i2 + 1];
                else
                    v = matrix[i1 * size2 + i2 + 1];

                if (v == 0)
                    break;

                builder.appendDeletion(i1, seq1.codeAt(i1));
                i1--;
            } else if (i2 >= 0 &&
                    v == alignYTOGapAfterX[(i1 + 1) * size2 + i2 + 1]) {

                if (v == alignYTOGapAfterX[(i1 + 1) * size2 + i2] + scoring.getGapExtensionPenalty())
                    v = alignYTOGapAfterX[(i1 + 1) * size2 + i2];
                else
                    v = matrix[(i1 + 1) * size2 + i2];

                if (v == 0)
                    break;

                builder.appendInsertion(i1 + 1, seq2.codeAt(i2));
                i2--;

            } else if (i1 >= 0 && i2 >= 0 && v == matrix[i1 * size2 + i2] + scoring.getScore(seq1.codeAt(i1), seq2.codeAt(i2))) {
                v = matrix[i1 * size2 + i2];

                if (seq1.codeAt(i1) != seq2.codeAt(i2))
                    builder.appendSubstitution(i1, seq1.codeAt(i1), seq2.codeAt(i2));

                if (v == 0)
                    break;

                --i1;
                --i2;
            } else
                throw new RuntimeException();

        }
        int seq1Start = i1;
        int seq2Start = i2;

        return new Alignment<>(seq1, builder.createAndDestroy(),
                new Range(seq1Start, i1Start), new Range(seq2Start, i2Start), max);
    }

}
