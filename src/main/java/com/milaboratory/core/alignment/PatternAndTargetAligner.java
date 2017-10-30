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

import com.milaboratory.core.Range;
import com.milaboratory.core.mutations.MutationsBuilder;
import com.milaboratory.core.sequence.*;

import static com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive.fromNucleotideSequence;

public final class PatternAndTargetAligner {
    private PatternAndTargetAligner() {
    }

    /**
     * Performs alignment of pattern and target with known position of pattern right side in the target and known
     * maximum allowed number of indels.
     *
     * @param scoring scoring system for pattern and target alignment
     * @param pattern pattern sequence, lowercase letters allow indels, uppercase letters don't allow
     * @param target full target sequence with quality
     * @param rightMatchPosition position of pattern right side in the target, inclusive
     * @param maxIndels maximum allowed number of insertions and deletions
     * @return alignment for pattern and target
     */
    public static Alignment<NucleotideSequenceCaseSensitive> alignLeftAdded(
            PatternAndTargetAlignmentScoring scoring, NucleotideSequenceCaseSensitive pattern,
            NSequenceWithQuality target, int rightMatchPosition, int maxIndels) {
        int leftMatchPosition = Math.max(0, rightMatchPosition + 1 - pattern.size() - maxIndels);
        if (leftMatchPosition < 0) leftMatchPosition = 0;
        int patternSize = pattern.size();
        int targetPartSize = rightMatchPosition - leftMatchPosition + 1;
        NucleotideSequenceCaseSensitive targetSequence = fromNucleotideSequence(target.getSequence(), true);
        SequenceQuality targetQuality = target.getQuality();

        try {
            MutationsBuilder<NucleotideSequenceCaseSensitive> builder = new MutationsBuilder<>(
                    NucleotideSequenceCaseSensitive.ALPHABET);
            CachedIntArray cachedArray = AlignmentCache.get();
            int matrixSize1 = patternSize + 1;
            int matrixSize2 = targetPartSize + 1;
            BandedMatrix matrix = new BandedMatrix(cachedArray, matrixSize1, matrixSize2, maxIndels);
            int i, j;

            for (i = matrix.getRowFactor() - matrix.getColumnDelta(); i > 0; i--)
                matrix.set(0, i, scoring.getGapPenalty(pattern, -1) * i);
            for (i = matrix.getColumnDelta(); i > 0; i--)
                matrix.set(i, 0, scoring.getGapPenalty(pattern, i - 1) * i);
            matrix.set(0, 0, 0);

            int match, delete, insert, to, gapPenalty;
            for (i = 0; i < patternSize; i++) {
                to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, targetPartSize);
                gapPenalty = scoring.getGapPenalty(pattern, i);
                for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; j++) {
                    match = matrix.get(i, j) +
                            scoring.getScore(pattern.codeAt(patternSize - 1 - i),
                                    targetSequence.codeAt(rightMatchPosition - j),
                                    targetQuality.value(rightMatchPosition - j));
                    delete = matrix.get(i, j + 1) + gapPenalty;
                    insert = matrix.get(i + 1, j) + gapPenalty;
                    matrix.set(i + 1, j + 1, Math.max(match, Math.max(delete, insert)));
                }
            }

            // searching for maxScore
            int maxI = -1, maxJ = -1;
            int maxScore = Integer.MIN_VALUE;

            if (maxScore < matrix.get(patternSize, targetPartSize)) {
                maxScore = matrix.get(patternSize, targetPartSize);
                maxI = patternSize;
                maxJ = targetPartSize;
            }

            i = patternSize;
            for (j = targetPartSize - maxIndels; j < matrixSize2; j++)
                if (maxScore < matrix.get(i, j)) {
                    maxScore = matrix.get(i, j);
                    maxI = i;
                    maxJ = j;
                }

            i = maxI - 1;
            j = maxJ - 1;
            gapPenalty = scoring.getGapPenalty(pattern, i);
            byte c1, c2;
            while (i >= 0 || j >= 0) {
                if (i >= 0 && j >= 0 &&
                        matrix.get(i + 1, j + 1) == matrix.get(i, j) +
                                scoring.getScore(c1 = pattern.codeAt(patternSize - 1 - i),
                                        c2 = targetSequence.codeAt(rightMatchPosition - j),
                                        targetQuality.value(rightMatchPosition - j))) {
                    if (c1 != c2)
                        builder.appendSubstitution(patternSize - 1 - i, c1, c2);
                    i--;
                    j--;
                    gapPenalty = scoring.getGapPenalty(pattern, i);
                } else if (i >= 0 &&
                        matrix.get(i + 1, j + 1) == matrix.get(i, j + 1) + gapPenalty) {
                    builder.appendDeletion(patternSize - 1 - i,
                            pattern.codeAt(patternSize - 1 - i));
                    i--;
                    gapPenalty = scoring.getGapPenalty(pattern, i);
                } else if (j >= 0 &&
                        matrix.get(i + 1, j + 1) == matrix.get(i + 1, j) + gapPenalty) {
                    builder.appendInsertion(patternSize - 1 - i,
                            targetSequence.codeAt(rightMatchPosition - j));
                    j--;
                } else
                    throw new RuntimeException();
            }

            int patternStop = patternSize - maxI;
            int targetStop = rightMatchPosition + 1 - maxJ;
            int score = scoring.calculateAlignmentScore(maxScore, patternSize);
            return new Alignment<>(pattern, builder.createAndDestroy(),
                    new Range(patternStop, patternSize), new Range(targetStop, rightMatchPosition + 1), score);
        } finally {
            AlignmentCache.release();
        }
    }

    /**
     * Performs global alignment of pattern and part of the target.
     *
     * @param scoring scoring system for pattern and target alignment
     * @param pattern pattern sequence, lowercase letters allow indels, uppercase letters don't allow
     * @param targetPart part of the target: sequence with quality
     * @return alignment for pattern and target part
     */
    public static Alignment<NucleotideSequenceCaseSensitive> alignGlobal(
            PatternAndTargetAlignmentScoring scoring, NucleotideSequenceCaseSensitive pattern,
            NSequenceWithQuality targetPart) {
        int patternSize = pattern.size();
        int targetPartSize = targetPart.size();
        NucleotideSequenceCaseSensitive targetPartSequence = fromNucleotideSequence(targetPart.getSequence(),
                true);
        SequenceQuality targetPartQuality = targetPart.getQuality();
        int matrixSize1 = patternSize + 1;
        int matrixSize2 = targetPartSize + 1;
        int matrix[] = new int[matrixSize1 * matrixSize2];
        int i1, i2, match, delete, insert;

        for (int i = 0; i < matrixSize2; i++)
            matrix[i] = scoring.getGapPenalty(pattern, -1) * i;
        for (int j = 1; j < matrixSize1; j++)
            matrix[matrixSize2 * j] = scoring.getGapPenalty(pattern, j - 1) * j;

        for (i1 = 0; i1 < patternSize; i1++) {
            int gapPenalty = scoring.getGapPenalty(pattern, i1);
            for (i2 = 0; i2 < targetPartSize; i2++) {
                match = matrix[i1 * matrixSize2 + i2] + scoring.getScore(
                        pattern.codeAt(i1), targetPartSequence.codeAt(i2), targetPartQuality.value(i2));
                delete = matrix[i1 * matrixSize2 + i2 + 1] + gapPenalty;
                insert = matrix[(i1 + 1) * matrixSize2 + i2] + gapPenalty;
                matrix[(i1 + 1) * matrixSize2 + i2 + 1] = Math.max(match, Math.max(delete, insert));
            }
        }

        MutationsBuilder<NucleotideSequenceCaseSensitive> builder = new MutationsBuilder<>(
                NucleotideSequenceCaseSensitive.ALPHABET, true);

        i1 = patternSize - 1;
        i2 = targetPartSize - 1;
        int score = scoring.calculateAlignmentScore(matrix[(i1 + 1) * matrixSize2 + i2 + 1], patternSize);
        int gapPenalty = scoring.getGapPenalty(pattern, i1);

        while (i1 >= 0 || i2 >= 0) {
            if (i1 >= 0 && i2 >= 0 &&
                    matrix[(i1 + 1) * matrixSize2 + i2 + 1] == matrix[i1 * matrixSize2 + i2] + scoring.getScore(
                            pattern.codeAt(i1), targetPartSequence.codeAt(i2), targetPartQuality.value(i2))) {
                if (pattern.codeAt(i1) != targetPartSequence.codeAt(i2))
                    builder.appendSubstitution(i1, pattern.codeAt(i1), targetPartSequence.codeAt(i2));
                i1--;
                i2--;
                gapPenalty = scoring.getGapPenalty(pattern, i1);
            } else if (i1 >= 0 &&
                    matrix[(i1 + 1) * matrixSize2 + i2 + 1] == matrix[i1 * matrixSize2 + i2 + 1] + gapPenalty) {
                builder.appendDeletion(i1, pattern.codeAt(i1));
                i1--;
                gapPenalty = scoring.getGapPenalty(pattern, i1);
            } else if (i2 >= 0 &&
                    matrix[(i1 + 1) * matrixSize2 + i2 + 1] == matrix[(i1 + 1) * matrixSize2 + i2] + gapPenalty) {
                builder.appendInsertion(i1 + 1, targetPartSequence.codeAt(i2));
                i2--;
            } else
                throw new RuntimeException();
        }

        return new Alignment<>(pattern, builder.createAndDestroy(),
                new Range(0, patternSize), new Range(0, targetPartSize), score);
    }
}