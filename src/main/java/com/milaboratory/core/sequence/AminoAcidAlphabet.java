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

/**
 * Amino acid alphabet with additional symbols.<br/> "~" - non full codon. 2 or 1 nucleotides.<br/> "-" - no nucleotides
 * <p>
 * This class is a singleton, and the access provided by
 * {@link com.milaboratory.core.sequence.AminoAcidSequence#ALPHABET}
 * or {@link #INSTANCE}.
 * </p>
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.Alphabet
 * @see com.milaboratory.core.sequence.AminoAcidSequence
 * @see com.milaboratory.core.sequence.Sequence
 */
public final class AminoAcidAlphabet extends AbstractArrayAlphabet<AminoAcidSequence> {
    static final AminoAcidAlphabet INSTANCE = new AminoAcidAlphabet();
    public static final byte Stop = 0;
    public static final byte A = 1;
    public static final byte C = 2;
    public static final byte D = 3;
    public static final byte E = 4;
    public static final byte F = 5;
    public static final byte G = 6;
    public static final byte H = 7;
    public static final byte I = 8;
    public static final byte K = 9;
    public static final byte L = 10;
    public static final byte M = 11;
    public static final byte N = 12;
    public static final byte P = 13;
    public static final byte Q = 14;
    public static final byte R = 15;
    public static final byte S = 16;
    public static final byte T = 17;
    public static final byte V = 18;
    public static final byte W = 19;
    public static final byte Y = 20;
    public static final byte IncompleteCodon = 21;

    private static final char[] aa = {
            '*',
            'A', 'C', 'D', 'E', 'F',
            'G', 'H', 'I', 'K', 'L',
            'M', 'N', 'P', 'Q', 'R',
            'S', 'T', 'V', 'W', 'Y',
            '_'};

    private AminoAcidAlphabet() {
        super("aminoacid", (byte) 2);
    }

    @Override
    public char symbolFromCode(byte code) {
        return aa[code];
    }

    @Override
    public byte codeFromSymbol(char symbol) {
        // Special case for backward compatibility
        if (symbol == '~')
            return IncompleteCodon;

        // For case insensitive conversion
        symbol = Character.toUpperCase(symbol);

        // Normal conversion (can be optimized :) )
        for (int i = 0; i < aa.length; ++i)
            if (aa[i] == symbol)
                return (byte) i;

        // Unknown symbol
        return -1;
    }

    @Override
    public int size() {
        return aa.length;
    }

    @Override
    AminoAcidSequence createUnsafe(byte[] array) {
        return new AminoAcidSequence(array, true);
    }
}
