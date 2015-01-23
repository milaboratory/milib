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
import com.milaboratory.util.IntArrayList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
     * @param codes    number of letters
     * @return simple substitution matrix
     */
    public static int[] getSymmetricMatrix(int match, int mismatch, int codes) {
        int[] matrix = new int[codes * codes];
        Arrays.fill(matrix, mismatch);
        for (int i = 0; i < codes; ++i)
            matrix[i + codes * i] = match;
        return matrix;
    }
}
