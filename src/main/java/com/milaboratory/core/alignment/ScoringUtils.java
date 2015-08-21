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

import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Wildcard;
import gnu.trove.set.hash.TByteHashSet;

import java.util.Arrays;

/**
 * ScoringUtils - class which contains some useful helpers used in scoring systems
 */
public final class ScoringUtils {
    private ScoringUtils() {
    }

    /**
     * Returns simple substitution matrix
     *
     * @param match    match score
     * @param mismatch mismatch score
     * @param alphabet alphabet
     * @return simple substitution matrix
     */
    public static int[] getSymmetricMatrix(int match, int mismatch, Alphabet<?> alphabet) {
        int codes = alphabet.size();
        int[] matrix = new int[codes * codes];
        Arrays.fill(matrix, mismatch);
        for (int i = 0; i < codes; ++i)
            matrix[i + codes * i] = match;
        return fillWildcardScores(matrix, alphabet);
    }

    /**
     * Fills up scores for wildcards by averaging scores for all their matching scores combinations.
     *
     * @param matrix   initial matrix (will be modified in-place)
     * @param alphabet alphabet
     * @param exclude  list of codes to exclude from this action
     * @return the same reference as matrix
     */
    public static int[] fillWildcardScores(int[] matrix, Alphabet<?> alphabet, byte... exclude) {
        int alSize = alphabet.size();

        if (matrix.length != alSize * alSize)
            throw new IllegalArgumentException("Wrong matrix size.");

        TByteHashSet excludeSet = new TByteHashSet(exclude);
        for (Wildcard wc1 : alphabet.getAllWildcards())
            for (Wildcard wc2 : alphabet.getAllWildcards()) {
                if ((wc1.isBasic() || excludeSet.contains(wc1.getCode())) && (wc2.isBasic() || excludeSet.contains(wc2.getCode())))
                    continue;
                int sumScore = 0;
                for (int i = 0; i < wc1.count(); i++)
                    for (int j = 0; j < wc2.count(); j++) {
                        sumScore += matrix[wc1.getMatchingCode(i) + wc2.getMatchingCode(j) * alSize];
                    }
                sumScore /= wc1.count() * wc2.count();
                matrix[wc1.getCode() + wc2.getCode() * alSize] = sumScore;
            }

        return matrix;
    }
}
