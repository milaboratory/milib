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

            matrix.set(0, 0, 0);
            int currentValue,
                    previousValue = 0;
            for (i = 1; i <= matrix.getColumnDelta(); i++) {
                currentValue = scoring.getGapPenalty(pattern, patternSize - i,
                        true, true) + previousValue;
                matrix.set(i, 0, currentValue);
                previousValue = currentValue;
            }
            for (j = 1; j <= matrix.getRowFactor() - matrix.getColumnDelta(); j++)
                matrix.set(0, j, scoring.getGapPenalty(pattern, patternSize,
                        true, false) * j);

            int match, delete, insert, to, gapPenaltyWithRight, gapPenaltyWithoutRight;
            for (i = 0; i < patternSize; i++) {
                to = Math.min(i + matrix.getRowFactor() - matrix.getColumnDelta() + 1, targetPartSize);
                gapPenaltyWithRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                        true, true);
                gapPenaltyWithoutRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                        true, false);
                for (j = Math.max(0, i - matrix.getColumnDelta()); j < to; j++) {
                    match = matrix.get(i, j) +
                            scoring.getScore(pattern.codeAt(patternSize - 1 - i),
                                    targetSequence.codeAt(rightMatchPosition - j),
                                    targetQuality.value(rightMatchPosition - j));
                    delete = matrix.get(i, j + 1) + gapPenaltyWithRight;
                    insert = matrix.get(i + 1, j) + gapPenaltyWithoutRight;
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
            gapPenaltyWithRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                    true, true);
            gapPenaltyWithoutRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                    true, false);
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
                    gapPenaltyWithRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                            true, true);
                    gapPenaltyWithoutRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                            true, false);
                } else if (i >= 0 &&
                        matrix.get(i + 1, j + 1) == matrix.get(i, j + 1) + gapPenaltyWithRight) {
                    builder.appendDeletion(patternSize - 1 - i,
                            pattern.codeAt(patternSize - 1 - i));
                    i--;
                    gapPenaltyWithRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                            true, true);
                    gapPenaltyWithoutRight = scoring.getGapPenalty(pattern, patternSize - 1 - i,
                            true, false);
                } else if (j >= 0 &&
                        matrix.get(i + 1, j + 1) == matrix.get(i + 1, j) + gapPenaltyWithoutRight) {
                    builder.appendInsertion(patternSize - 1 - i,
                            targetSequence.codeAt(rightMatchPosition - j));
                    j--;
                } else
                    throw new RuntimeException();
            }

            int patternStop = patternSize - maxI;
            int targetStop = rightMatchPosition + 1 - maxJ;
            return new Alignment<>(pattern, builder.createAndDestroy(),
                    new Range(patternStop, patternSize), new Range(targetStop, rightMatchPosition + 1), maxScore);
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

        matrix[0] = 0;
        for (i1 = 1; i1 < matrixSize1; i1++)
            matrix[i1 * matrixSize2] = matrix[(i1 - 1) * matrixSize2]
                    + scoring.getGapPenalty(pattern, i1 - 1, true, true);
        for (i2 = 1; i2 < matrixSize2; i2++)
            matrix[i2] = scoring.getGapPenalty(pattern, -1, false, true) * i2;

        for (i1 = 0; i1 < patternSize; i1++) {
            int gapPenaltyWithLeft = scoring.getGapPenalty(pattern, i1,
                    true, true);
            int gapPenaltyWithoutLeft = scoring.getGapPenalty(pattern, i1,
                    false, true);
            for (i2 = 0; i2 < targetPartSize; i2++) {
                match = matrix[i1 * matrixSize2 + i2] + scoring.getScore(
                        pattern.codeAt(i1), targetPartSequence.codeAt(i2), targetPartQuality.value(i2));
                delete = matrix[i1 * matrixSize2 + i2 + 1] + gapPenaltyWithLeft;
                insert = matrix[(i1 + 1) * matrixSize2 + i2] + gapPenaltyWithoutLeft;
                matrix[(i1 + 1) * matrixSize2 + i2 + 1] = Math.max(match, Math.max(delete, insert));
            }
        }

        MutationsBuilder<NucleotideSequenceCaseSensitive> builder = new MutationsBuilder<>(
                NucleotideSequenceCaseSensitive.ALPHABET, true);

        i1 = patternSize - 1;
        i2 = targetPartSize - 1;
        int score = matrix[(i1 + 1) * matrixSize2 + i2 + 1];
        int gapPenaltyWithLeft = scoring.getGapPenalty(pattern, i1,
                true, true);
        int gapPenaltyWithoutLeft = scoring.getGapPenalty(pattern, i1,
                false, true);

        while (i1 >= 0 || i2 >= 0) {
            if (i1 >= 0 && i2 >= 0 &&
                    matrix[(i1 + 1) * matrixSize2 + i2 + 1] == matrix[i1 * matrixSize2 + i2] + scoring.getScore(
                            pattern.codeAt(i1), targetPartSequence.codeAt(i2), targetPartQuality.value(i2))) {
                if (pattern.codeAt(i1) != targetPartSequence.codeAt(i2))
                    builder.appendSubstitution(i1, pattern.codeAt(i1), targetPartSequence.codeAt(i2));
                i1--;
                i2--;
                gapPenaltyWithLeft = scoring.getGapPenalty(pattern, i1,
                        true, true);
                gapPenaltyWithoutLeft = scoring.getGapPenalty(pattern, i1,
                        false, true);
            } else if (i1 >= 0 &&
                    matrix[(i1 + 1) * matrixSize2 + i2 + 1] == matrix[i1 * matrixSize2 + i2 + 1]
                            + gapPenaltyWithLeft) {
                builder.appendDeletion(i1, pattern.codeAt(i1));
                i1--;
                gapPenaltyWithLeft = scoring.getGapPenalty(pattern, i1,
                        true, true);
                gapPenaltyWithoutLeft = scoring.getGapPenalty(pattern, i1,
                        false, true);
            } else if (i2 >= 0 &&
                    matrix[(i1 + 1) * matrixSize2 + i2 + 1] == matrix[(i1 + 1) * matrixSize2 + i2]
                            + gapPenaltyWithoutLeft) {
                builder.appendInsertion(i1 + 1, targetPartSequence.codeAt(i2));
                i2--;
            } else
                throw new RuntimeException();
        }

        return new Alignment<>(pattern, builder.createAndDestroy(),
                new Range(0, patternSize), new Range(0, targetPartSize), score);
    }
}
