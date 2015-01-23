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

import com.milaboratory.core.sequence.AminoAcidSequence;
import com.milaboratory.core.sequence.IncompleteAminoAcidSequence;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

import static com.milaboratory.core.alignment.ScoringMatrixIO.readAABlastMatrix;

public class ScoringMatrixIOTest {
    @Test(expected = IllegalArgumentException.class)
    public void testReadMatrix2() throws Exception {
        try (InputStream stream = ScoringMatrixIO.class.getClassLoader().getResourceAsStream("matrices/PAM70")) {
            int[] matrix = readAABlastMatrix(stream, AminoAcidSequence.ALPHABET);
        }
    }

    @Test
    public void testReadMatrix3() throws Exception {
        try (InputStream stream = ScoringMatrixIO.class.getClassLoader().getResourceAsStream("matrices/PAM70")) {
            int[] matrix = readAABlastMatrix(stream, IncompleteAminoAcidSequence.ALPHABET, '_', '.');
            int sum = 0;
            for (int i : matrix)
                sum += i;
            Assert.assertEquals(-2057, sum);
        }
    }
}