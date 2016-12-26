/*
 * Copyright 2016 MiLaboratory.com
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
import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.mutations.MutationsBuilder;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;

/**
 * Collection of custom aligners
 */
public class AlignerCustom {
    public static <S extends Sequence<S>> Alignment<S> alignLinearSemiLocalLeft0(LinearGapAlignmentScoring<S> scoring,
                                                                                 S seq1, S seq2,
                                                                                 int offset1, int length1,
                                                                                 int offset2, int length2,
                                                                                 boolean boundSeq1, boolean boundSeq2,
                                                                                 Alphabet<S> alphabet,
                                                                                 LinearMatrixCache cache) {
        final int size1 = length1 + 1,
                size2 = length2 + 1;

        Matrix matrix = cache.getMatrix(size1, size2);

        int i, j;

        if (boundSeq1)
            for (i = 1; i < size1; ++i)
                matrix.set(i, 0, scoring.getGapPenalty() * i);
        else
            for (i = 1; i < size1; ++i)
                matrix.set(i, 0, 0);

        if (boundSeq2)
            for (i = 1; i < size2; ++i)
                matrix.set(0, i, scoring.getGapPenalty() * i);
        else
            for (i = 1; i < size2; ++i)
                matrix.set(0, i, 0);

        matrix.set(0, 0, 0);

        int match, delete, insert;
        int max = 0;
        int iStop = 0, jStop = 0;

        for (i = 0; i < length1; ++i)
            for (j = 0; j < length2; ++j) {
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
            }

        MutationsBuilder<S> mutations = new MutationsBuilder<>(alphabet);

        i = iStop - 1;
        j = jStop - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i == -1 && !boundSeq2)
                break;
            if (j == -1 && !boundSeq1)
                break;
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

        mutations.reverseRange(0, mutations.size());

        return new Alignment<>(seq1, mutations.createAndDestroy(),
                new Range(offset1 + i + 1, offset1 + iStop),
                new Range(offset2 + j + 1, offset2 + jStop),
                max);
    }

    public static <S extends Sequence<S>> Alignment<S> alignLinearSemiLocalRight0(LinearGapAlignmentScoring<S> scoring,
                                                                                  S seq1, S seq2,
                                                                                  int offset1, int length1,
                                                                                  int offset2, int length2,
                                                                                  boolean boundSeq1, boolean boundSeq2,
                                                                                  Alphabet<S> alphabet,
                                                                                  LinearMatrixCache cache) {
        int size1 = length1 + 1,
                size2 = length2 + 1;

        Matrix matrix = cache.getMatrix(size1, size2);

        int i, j;

        if (boundSeq1)
            for (i = 1; i < size1; ++i)
                matrix.set(i, 0, scoring.getGapPenalty() * i);
        else
            for (i = 1; i < size1; ++i)
                matrix.set(i, 0, 0);

        if (boundSeq2)
            for (i = 1; i < size2; ++i)
                matrix.set(0, i, scoring.getGapPenalty() * i);
        else
            for (i = 1; i < size2; ++i)
                matrix.set(0, i, 0);

        matrix.set(0, 0, 0);

        int match, delete, insert;
        int max = 0;
        int iStop = 0, jStop = 0;

        for (i = 0; i < length1; ++i)
            for (j = 0; j < length2; ++j) {
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
            }

        MutationsBuilder<S> mutations = new MutationsBuilder<>(alphabet);

        i = iStop - 1;
        j = jStop - 1;
        byte c1, c2;
        while (i >= 0 || j >= 0) {
            if (i == -1 && !boundSeq2)
                break;
            if (j == -1 && !boundSeq1)
                break;
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

        return new Alignment<>(seq1, mutations.createAndDestroy(),
                new Range(offset1 + length1 - iStop, offset1 + length1 - i - 1),
                new Range(offset2 + length2 - jStop, offset2 + length2 - j - 1),
                max);
    }

    public static final int MIN_VALUE = Integer.MIN_VALUE / 2;

    public static <S extends Sequence<S>> Alignment<S> alignAffineSemiLocalLeft0(AffineGapAlignmentScoring<S> scoring,
                                                                                 S seq1, S seq2,
                                                                                 int offset1, int length1,
                                                                                 int offset2, int length2,
                                                                                 boolean boundSeq1, boolean boundSeq2,
                                                                                 Alphabet<S> alphabet,
                                                                                 AffineMatrixCache cache) {
        if (length1 == 0 || length2 == 0)
            return new Alignment<>(seq1, Mutations.empty(alphabet), new Range(offset1, offset1), new Range(offset2, offset2), 0);

        int size1 = length1 + 1,
                size2 = length2 + 1;

        cache.initMatrices(size1, size2);

        Matrix main = cache.main;
        Matrix gapIn1 = cache.gapIn1;
        Matrix gapIn2 = cache.gapIn2;

        int i, j;

        for (i = 1; i < size1; ++i) {
            //int v = boundSeq1 ?
            //        scoring.getGapOpenPenalty() + scoring.getGapExtensionPenalty() * (i - 1) :
            //        0;
            int v = scoring.getGapOpenPenalty() + scoring.getGapExtensionPenalty() * (i - 1);
            main.set(i, 0, boundSeq1 ? v : 0);
            gapIn1.set(i, 0, MIN_VALUE);
            gapIn2.set(i, 0, v);
        }

        for (i = 1; i < size2; ++i) {
            //int v = boundSeq2 ?
            //        scoring.getGapOpenPenalty() + scoring.getGapExtensionPenalty() * (i - 1) :
            //        0;
            int v = scoring.getGapOpenPenalty() + scoring.getGapExtensionPenalty() * (i - 1);
            main.set(0, i, boundSeq2 ? v : 0);
            gapIn1.set(0, i, v);
            gapIn2.set(0, i, MIN_VALUE);
        }

        main.set(0, 0, 0);
        gapIn1.set(0, 0, MIN_VALUE);
        gapIn2.set(0, 0, MIN_VALUE);

        int match, gap1, gap2;

        int maxI = -1, maxJ = -1, maxScore = 0;
        final int gapExtensionPenalty = scoring.getGapExtensionPenalty();

        for (i = 0; i < length1; ++i) {
            for (j = 0; j < length2; ++j) {
                match = main.get(i, j) +
                        scoring.getScore(seq1.codeAt(offset1 + i), seq2.codeAt(offset2 + j));

                gap1 = Math.max(main.get(i + 1, j) + scoring.getGapOpenPenalty(), gapIn1.get(i + 1, j) + gapExtensionPenalty);
                gap2 = Math.max(main.get(i, j + 1) + scoring.getGapOpenPenalty(), gapIn2.get(i, j + 1) + gapExtensionPenalty);

                gapIn1.set(i + 1, j + 1, gap1);
                gapIn2.set(i + 1, j + 1, gap2);
                int score = Math.max(match, Math.max(gap1, gap2));
                main.set(i + 1, j + 1, score);

                if (score > maxScore) {
                    maxScore = score;
                    maxI = i;
                    maxJ = j;
                }
            }
        }

        MutationsBuilder<S> mutations = new MutationsBuilder<>(alphabet);

        i = maxI;
        j = maxJ;
        int pScore = main.get(i + 1, j + 1);

        byte c1, c2;
        while (i >= 0 || j >= 0) {
            //if (i == -1 && !boundSeq2)
            //    break;
            //if (j == -1 && !boundSeq1)
            //    break;
            if (i >= 0 &&
                    pScore == gapIn2.get(i + 1, j + 1)) {
                if (pScore == gapIn2.get(i, j + 1) + gapExtensionPenalty)
                    pScore = gapIn2.get(i, j + 1);
                else
                    pScore = main.get(i, j + 1);

                mutations.appendDeletion(offset1 + i, seq1.codeAt(offset1 + i));
                --i;
            } else if (j >= 0 &&
                    pScore == gapIn1.get(i + 1, j + 1)) {
                if (pScore == gapIn1.get(i + 1, j) + gapExtensionPenalty)
                    pScore = gapIn1.get(i + 1, j);
                else
                    pScore = main.get(i + 1, j);

                mutations.appendInsertion(offset1 + i + 1, seq2.codeAt(offset2 + j));
                --j;
            } else if (i >= 0 && j >= 0 &&
                    pScore == main.get(i, j) + scoring.getScore(c1 = seq1.codeAt(offset1 + i), c2 = seq2.codeAt(offset2 + j))) {
                pScore = main.get(i, j);
                if (c1 != c2)
                    mutations.appendSubstitution(offset1 + i, c1, c2);
                --i;
                --j;
            } else{
                if (i == -1 && !boundSeq2)
                    break;
                if (j == -1 && !boundSeq1)
                    break;
                throw new RuntimeException();
            }
        }

        mutations.reverseRange(0, mutations.size());

        return new Alignment<>(seq1, mutations.createAndDestroy(),
                new Range(offset1 + i + 1, offset1 + maxI + 1),
                new Range(offset2 + j + 1, offset2 + maxJ + 1),
                maxScore);
    }

    public static final class Matrix {
        /**
         * Data
         */
        private final int[] data;
        private final int height, width;

        public Matrix(int[] data, int height, int width) {
            this.data = data;
            this.height = height;
            this.width = width;
        }

        public int get(int row, int col) {
            return data[width * row + col];
        }

        public void set(int row, int col, int value) {
            data[width * row + col] = value;
        }
    }

    public interface MatrixCache {}

    public static final class LinearMatrixCache implements MatrixCache {
        final CachedIntArray cache = new CachedIntArray();

        Matrix getMatrix(int height, int width) {
            return new Matrix(cache.get(height * width), height, width);
        }
    }

    public static final class AffineMatrixCache implements MatrixCache {
        private final CachedIntArray mainCache = new CachedIntArray(),
                gapIn1Cache = new CachedIntArray(),
                gapIn2Cache = new CachedIntArray();
        Matrix main, gapIn1, gapIn2;

        void initMatrices(int height, int width) {
            this.main = new Matrix(mainCache.get(height * width), height, width);
            this.gapIn1 = new Matrix(gapIn1Cache.get(height * width), height, width);
            this.gapIn2 = new Matrix(gapIn2Cache.get(height * width), height, width);
        }
    }
}
