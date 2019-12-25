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
package com.milaboratory.core.sequence;

import java.util.Arrays;

/**
 * Defines standard genetic code.
 */
public final class GeneticCode {
    private static byte[] basicCode = null;
    private static byte[] wildcardCode = null;

    static {
        char[] Base1 = "ttttttttttttttttccccccccccccccccaaaaaaaaaaaaaaaagggggggggggggggg".toCharArray();
        char[] Base2 = "ttttccccaaaaggggttttccccaaaaggggttttccccaaaaggggttttccccaaaagggg".toCharArray();
        char[] Base3 = "tcagtcagtcagtcagtcagtcagtcagtcagtcagtcagtcagtcagtcagtcagtcagtcag".toCharArray();
        char[] Amino = "FFLLSSSSYY**CC*WLLLLPPPPHHQQRRRRIIIMTTTTNNKKSSRRVVVVAAAADDEEGGGG".toCharArray();
        basicCode = new byte[Base1.length];
        int triplet;
        byte b0, b1, b2;
        for (int i = 0; i < Base1.length; ++i) {
            b0 = NucleotideAlphabet.INSTANCE.symbolToCode(Base1[i]);
            b1 = NucleotideAlphabet.INSTANCE.symbolToCode(Base2[i]);
            b2 = NucleotideAlphabet.INSTANCE.symbolToCode(Base3[i]);
            triplet = (b0 << 4) | (b1 << 2) | b2;
            basicCode[triplet] = AminoAcidAlphabet.INSTANCE.symbolToCode(Amino[i]);
        }

        Base1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTYYYMMM".toCharArray();
        Base2 = "AAAAAAGGGGGGCCCCCCCCCCCCCCCTTTTTTTTAAAAAAGGGGGGGGGGGGGGGCCCCCCCCCCCCCCCTTTTTTTTTTTTTTTAAAAAAGGGGGGGGGGGGGGGCCCCCCCCCCCCCCCTTTTTTTTTTTTTTTAAAAAAGGGGGCCCCCCCCCCCCCCCTTTTTTRTTTGGG".toCharArray();
        Base3 = "AGCTRYAGCTRYAGCTNRYSWKMBDHVAGCTYWMHAGCTRYAGCTNRYSWKMBDHVAGCTNRYSWKMBDHVAGCTNRYSWKMBDHVAGCTRYAGCTNRYSWKMBDHVAGCTNRYSWKMBDHVAGCTNRYSWKMBDHVAGCTRYAGCTYAGCTNRYSWKMBDHVAGCTRYAAGRAGR".toCharArray();
        Amino = "KKNNKNRRSSRSTTTTTTTTTTTTTTTIMIIIIIIEEDDEDGGGGGGGGGGGGGGGAAAAAAAAAAAAAAAVVVVVVVVVVVVVVVQQHHQHRRRRRRRRRRRRRRRPPPPPPPPPPPPPPPLLLLLLLLLLLLLLL**YY*Y*WCCCSSSSSSSSSSSSSSSLLFFLF*LLLRRR".toCharArray();
        wildcardCode = new byte[1 << 12];
        Arrays.fill(wildcardCode, AminoAcidAlphabet.X);
        for (int i = 0; i < Base1.length; ++i) {
            b0 = NucleotideAlphabet.INSTANCE.symbolToCode(Base1[i]);
            b1 = NucleotideAlphabet.INSTANCE.symbolToCode(Base2[i]);
            b2 = NucleotideAlphabet.INSTANCE.symbolToCode(Base3[i]);
            triplet = (b0 << 8) | (b1 << 4) | b2;
            wildcardCode[triplet] = AminoAcidAlphabet.INSTANCE.symbolToCode(Amino[i]);
        }
    }

    public static byte getAminoAcid(byte n0, byte n1, byte n2) {
        return getAminoAcid(n0 << 8 | n1 << 4 | n2);
    }

    static byte getAminoAcid(int triplet) {
        return wildcardCode[triplet];
    }

    public static void translate(byte[] dest, int offsetInDest, NucleotideSequence sequence, int offsetInSeq, int seqLength) {
        if (seqLength % 3 != 0)
            throw new IllegalArgumentException("Only nucleotide sequences with size multiple " +
                    "of three are supported (in-frame).");

        int size = seqLength / 3;
        int triplet;
        for (int i = 0; i < size; i++) {
            triplet = (sequence.codeAt(offsetInSeq + i * 3) << 8) |
                    (sequence.codeAt(offsetInSeq + i * 3 + 1) << 4) |
                    sequence.codeAt(offsetInSeq + i * 3 + 2);
            dest[i + offsetInDest] = wildcardCode[triplet];
        }
    }


    public static byte getBasicAminoAcid(byte n0, byte n1, byte n2) {
        return getBasicAminoAcid(n0 << 4 | n1 << 2 | n2);
    }

    static byte getBasicAminoAcid(int triplet) {
        return basicCode[triplet];
    }

    public static void basicTranslate(byte[] dest, int offsetInDest, NucleotideSequence sequence, int offsetInSeq, int seqLength) {
        if (seqLength % 3 != 0)
            throw new IllegalArgumentException("Only nucleotide sequences with size multiple " +
                    "of three are supported (in-frame).");
        if (sequence.containsWildcards(offsetInSeq, offsetInSeq + seqLength))
            throw new IllegalArgumentException("Nucleotide sequences with wildcards are not supported.");

        int size = seqLength / 3;
        int triplet;
        for (int i = 0; i < size; i++) {
            triplet = (sequence.codeAt(offsetInSeq + i * 3) << 4) |
                    (sequence.codeAt(offsetInSeq + i * 3 + 1) << 2) |
                    sequence.codeAt(offsetInSeq + i * 3 + 2);
            dest[i + offsetInDest] = basicCode[triplet];
        }
    }
}
