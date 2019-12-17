/*
 * Copyright 2019 MiLaboratory, LLC
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
package com.milaboratory.core.sequence;

import gnu.trove.set.hash.TByteHashSet;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class GeneticCodeTest {
    @Test
    @Ignore
    public void generateExtendedGeneticCode() {
        StringBuilder
                n1 = new StringBuilder(),
                n2 = new StringBuilder(),
                n3 = new StringBuilder(),
                aa = new StringBuilder();

        for (Wildcard iw : NucleotideSequence.ALPHABET.getAllWildcards())
            for (Wildcard jw : NucleotideSequence.ALPHABET.getAllWildcards())
                for (Wildcard kw : NucleotideSequence.ALPHABET.getAllWildcards()) {
                    TByteHashSet aminoAcids = new TByteHashSet();
                    for (byte i : iw.basicMatchingCodes)
                        for (byte j : jw.basicMatchingCodes)
                            for (byte k : kw.basicMatchingCodes)
                                aminoAcids.add(GeneticCode.getBasicAminoAcid(i, j, k));
                    if (aminoAcids.size() == 1) {
                        System.out.println("" + iw.getSymbol() + jw.getSymbol() + kw.getSymbol() + " -> " + AminoAcidSequence.ALPHABET.codeToSymbol(aminoAcids.iterator().next()));
                        n1.append(iw.getSymbol());
                        n2.append(jw.getSymbol());
                        n3.append(kw.getSymbol());
                        aa.append(AminoAcidSequence.ALPHABET.codeToSymbol(aminoAcids.iterator().next()));
                    }
                }

        System.out.println(n1);
        System.out.println(n2);
        System.out.println(n3);
        System.out.println(aa);
    }

    @Test
    public void test1() {
        for (byte i = 0; i < 4; i++)
            for (byte j = 0; j < 4; j++)
                for (byte k = 0; k < 4; k++)
                    Assert.assertEquals(GeneticCode.getAminoAcid(i, j, k), GeneticCode.getBasicAminoAcid(i, j, k));
    }
}