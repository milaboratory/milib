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

import gnu.trove.map.hash.TCharObjectHashMap;

import java.util.Collection;
import java.util.Collections;

/**
 * An alphabet for nucleotide sequences. This alphabet defines the following mapping:
 * <br>0 - 'A', 1 - 'G', 2 - 'C', 3 - 'T'.
 * <p>
 * This class is a singleton, and the access provided by
 * {@link com.milaboratory.core.sequence.NucleotideSequence#ALPHABET}
 * or {@link #INSTANCE}.
 * </p>
 * <p>
 * Nucleotide alphabet contains wildcards as specified in e.g. FASTA format: 'R' for 'A' or 'G',
 * 'Y' for 'C' or 'T' etc.
 * </p>
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.WithWildcards
 * @see com.milaboratory.core.sequence.Alphabet
 * @see com.milaboratory.core.sequence.NucleotideSequence
 */
public final class NucleotideAlphabet extends Alphabet<NucleotideSequence> implements WithWildcards {
    /**
     * Adenine byte
     */
    public static final byte A = 0x00;
    /**
     * Guanine byte
     */
    public static final byte G = 0x01;
    /**
     * Cytosine byte
     */
    public static final byte C = 0x02;
    /**
     * Thymine byte
     */
    public static final byte T = 0x03;
    private static char[] chars = {'A', 'G', 'C', 'T'};
    private static byte[] bytes = {'A', 'G', 'C', 'T'};
    /**
     * Singleton instance.
     */
    final static NucleotideAlphabet INSTANCE = new NucleotideAlphabet();
    private static TCharObjectHashMap<WildcardSymbol> wildcardsMap =
            new WildcardMapBuilder()
                    /* Exact match letters */
                    .addAlphabet(INSTANCE)
                    /* Two-letter wildcard */
                    .addWildcard('R', A, G)
                    .addWildcard('Y', C, T)
                    .addWildcard('S', G, C)
                    .addWildcard('W', A, T)
                    .addWildcard('K', G, T)
                    .addWildcard('M', A, C)
                    /* Three-letter wildcard */
                    .addWildcard('B', C, G, T)
                    .addWildcard('D', A, G, T)
                    .addWildcard('H', A, C, T)
                    .addWildcard('V', A, C, G)
                    /* N */
                    .addWildcard('N', A, T, G, C)
                    .get();

    private NucleotideAlphabet() {
        super("nucleotide", (byte) 1);
    }

    /**
     * Returns a complement nucleotide.
     *
     * @param nucleotide byte value of nucleotide
     * @return complement nucleotide to the specified one
     */
    public static byte getComplement(byte nucleotide) {
        return (byte) (nucleotide ^ 3);
    }

    @Override
    public byte codeFromSymbol(char symbol) {
        switch (symbol) {
            case 'a':
            case 'A':
                return (byte) 0x00;
            case 't':
            case 'T':
                return (byte) 0x03;
            case 'g':
            case 'G':
                return (byte) 0x01;
            case 'c':
            case 'C':
                return (byte) 0x02;
        }
        return -1;
    }

    /**
     * Returns a byte-code for UTF-8 nucleotide symbol(i.e. {@code symbol} is a character that is
     * casted to byte, but not a real nucleotide byte-code).
     *
     * @param symbol letter
     * @return byte-code for nucleotide letter or -1 if this letter does not represent any nucleotide.
     */
    public static byte codeFromSymbolByte(byte symbol) {
        switch (symbol) {
            case (byte) 'a':
            case (byte) 'A':
                return (byte) 0x00;
            case (byte) 't':
            case (byte) 'T':
                return (byte) 0x03;
            case (byte) 'g':
            case (byte) 'G':
                return (byte) 0x01;
            case (byte) 'c':
            case (byte) 'C':
                return (byte) 0x02;
        }
        return -1;
    }

    /**
     * Returns UTF-8 character corresponding to specified byte-code.
     *
     * @param code byte-code of nucleotide
     * @return UTF-8 character corresponding to specified byte-code
     */
    public static byte symbolByteFromCode(byte code) {
        if (code < 0 || code >= chars.length)
            throw new IllegalArgumentException("Illegal byte-code of nucleotide.");
        return bytes[code];
    }

    /**
     * Returns a letter corresponding to specified byte-code.
     *
     * @param code byte-code of nucleotide
     * @return letter corresponding to specified byte-code
     */
    public char symbolFromCode(byte code) {
        if (code < 0 || code >= chars.length)
            throw new IllegalArgumentException("Illegal byte-code of nucleotide.");
        return chars[code];
    }

    @Override
    public int size() {
        return 4;
    }

    @Override
    public Collection<WildcardSymbol> getAllWildcards() {
        return Collections.unmodifiableCollection(wildcardsMap.valueCollection());
    }

    @Override
    public WildcardSymbol getWildcardFor(char symbol) {
        return wildcardsMap.get(symbol);
    }

    @Override
    public NucleotideSequenceBuilder getBuilder() {
        return new NucleotideSequenceBuilder();
    }
}
