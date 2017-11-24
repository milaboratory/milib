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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.milaboratory.core.Range;

@JsonSerialize(using = IO.NSeqSerializer.class)
@JsonDeserialize(using = IO.NSeqDeserializer.class)
public final class NucleotideSequenceCaseSensitive extends AbstractArraySequence<NucleotideSequenceCaseSensitive>
        implements NSeq<NucleotideSequenceCaseSensitive>, java.io.Serializable  {
    /**
     * Nucleotide alphabet
     */
    public static final NucleotideAlphabetCaseSensitive ALPHABET = NucleotideAlphabetCaseSensitive.INSTANCE;

    /**
     * Empty instance
     */
    public static final NucleotideSequenceCaseSensitive EMPTY = new NucleotideSequenceCaseSensitive("");

    /**
     * Creates nucleotide sequence from its string representation (e.g. "ATCGG" or "atcgg").
     *
     * @param sequence string representation of sequence (case sensitive)
     * @throws java.lang.IllegalArgumentException if sequence contains unknown nucleotide symbol
     */
    public NucleotideSequenceCaseSensitive(String sequence) {
        super(sequence, true);
    }

    /**
     * Creates nucleotide sequence from char array of nucleotides (e.g. ['A','t','c','g','G']).
     *
     * @param sequence char array of nucleotides (case sensitive)
     * @throws java.lang.IllegalArgumentException if sequence contains unknown nucleotide symbol
     */
    public NucleotideSequenceCaseSensitive(char[] sequence) {
        super(sequence, true);
    }

    /**
     * Creates nucleotide sequence from specified {@code Bit2Array} (will be copied in constructor).
     *
     * @param data Bit2Array
     */
    public NucleotideSequenceCaseSensitive(byte[] data) {
        super(data.clone());
    }

    NucleotideSequenceCaseSensitive(byte[] data, boolean unsafe) {
        super(data);
        assert unsafe;
    }

    @Override
    public NucleotideSequenceCaseSensitive getRange(Range range) {
        if (range.getLower() < 0 || range.getUpper() < 0
                || range.getLower() > size() || range.getUpper() > size())
            throw new IndexOutOfBoundsException();

        if (range.length() == 0)
            return EMPTY;

        if (range.isReverse())
            throw new IllegalArgumentException("NucleotideSequenceCaseSensitive doesn't support reversed ranges!");
        else
            return super.getRange(range);
    }

    @Override
    public NucleotideSequenceCaseSensitive getReverseComplement() {
        throw new IllegalStateException("getReverseComplement() not supported in NucleotideSequenceCaseSensitive!");
    }

    /**
     * Creates case sensitive nucleotide sequence from specified byte array.
     *
     * @param sequence byte array
     * @param offset   offset in {@code sequence}
     * @param length   length of resulting sequence
     * @return case sensitive nucleotide sequence
     */
    public static NucleotideSequenceCaseSensitive fromSequence(byte[] sequence, int offset, int length) {
        byte[] storage = new byte[length];
        for (int i = 0; i < length; ++i)
            storage[i] = ALPHABET.symbolToCodeCaseSensitive((char) sequence[offset + i]);
        return new NucleotideSequenceCaseSensitive(storage, true);
    }

    /**
     * Converts NucleotideSequence to NucleotideSequenceCaseSensitive.
     *
     * @param sequence nucleotide sequence
     * @param lowercase if true, resulting sequence will be lowercase, otherwise uppercase
     * @return case sensitive nucleotide sequence
     */
    public static NucleotideSequenceCaseSensitive fromNucleotideSequence(NucleotideSequence sequence,
            boolean lowercase) {
        int length = sequence.size();
        byte[] storage = new byte[length];
        if (lowercase)
            for (int i = 0; i < length; ++i)
                storage[i] = ALPHABET.symbolToCodeCaseSensitive(Character.toLowerCase(sequence.symbolAt(i)));
        else
            for (int i = 0; i < length; ++i)
                storage[i] = ALPHABET.symbolToCodeCaseSensitive(Character.toUpperCase(sequence.symbolAt(i)));

        return new NucleotideSequenceCaseSensitive(storage, true);
    }

    /**
     * Converts this NucleotideSequenceCaseSensitive to NucleotideSequence with losing information about case.
     *
     * @return nucleotide sequence
     */
    public NucleotideSequence toNucleotideSequence() {
        byte[] caseInsensitiveData = new byte[data.length];
        for (int i = 0; i < data.length; i++)
            caseInsensitiveData[i] = NucleotideAlphabet.INSTANCE.symbolToCode(ALPHABET.codeToSymbol(data[i]));
        return new NucleotideSequence(caseInsensitiveData);
    }

    /**
     * Returns {@literal true} if sequence contains wildcards in specified region.
     *
     * @return {@literal true} if sequence contains wildcards in specified region
     */
    public boolean containsWildcards(int from, int to) {
        for (int i = from; i < to; i++)
            if (isWildcard(codeAt(i)))
                return true;
        return false;
    }

    /**
     * Returns {@literal true} if sequence contains wildcards.
     *
     * @return {@literal true} if sequence contains wildcards
     */
    public boolean containsWildcards() {
        return containsWildcards(0, size());
    }

    @Override
    public AbstractArrayAlphabet<NucleotideSequenceCaseSensitive> getAlphabet() {
        return ALPHABET;
    }

    private static boolean isWildcard(byte nucleotide) {
        return nucleotide >= 8;
    }
}
