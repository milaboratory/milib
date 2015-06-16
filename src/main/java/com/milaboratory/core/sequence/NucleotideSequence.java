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

import com.milaboratory.core.Range;
import com.milaboratory.primitivio.annotations.Serializable;
import com.milaboratory.util.Bit2Array;

/**
 * Representation of nucleotide sequence. <p> Implementation note: the array of data is packed into a bit array ({@link
 * com.milaboratory.util.Bit2Array}) for memory saving. </p>
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.Sequence
 * @see com.milaboratory.core.sequence.NucleotideAlphabet
 */
@Serializable(by = IO.NucleotideSequenceSerializer.class)
public final class NucleotideSequence extends Sequence<NucleotideSequence>
        implements NSeq<NucleotideSequence>, java.io.Serializable {
    /**
     * Empty instance
     */
    public static final NucleotideSequence EMPTY = new NucleotideSequence("");
    /**
     * Nucleotide alphabet
     */
    public static final NucleotideAlphabet ALPHABET = NucleotideAlphabet.INSTANCE;
    final Bit2Array data;
    private static final long serialVersionUID = 1L;

    /**
     * Creates nucleotide sequence from its string representation (e.g. "ATCGG" or "atcgg").
     *
     * @param sequence string representation of sequence (case insensitive)
     * @throws java.lang.IllegalArgumentException if sequence contains unknown nucleotide symbol
     */
    public NucleotideSequence(String sequence) {
        data = new Bit2Array(sequence.length());
        for (int i = 0; i < sequence.length(); ++i) {
            byte code = ALPHABET.codeFromSymbol(sequence.charAt(i));
            if (code == -1)
                throw new IllegalArgumentException("Unknown nucleotide: \"" + sequence.charAt(i) + "\".");
            data.set(i, code);
        }
    }

    /**
     * Creates nucleotide sequence from char array of nucleotides (e.g. ['A','T','C','G','G']).
     *
     * @param sequence char array of nucleotides
     * @throws java.lang.IllegalArgumentException if sequence contains unknown nucleotide symbol
     */
    public NucleotideSequence(char[] sequence) {
        data = new Bit2Array(sequence.length);
        for (int i = 0; i < sequence.length; ++i) {
            byte code = ALPHABET.codeFromSymbol(sequence[i]);
            if (code == -1)
                throw new IllegalArgumentException("Unknown nucleotide: \"" + sequence[i] + "\".");
            data.set(i, code);
        }
    }

    /**
     * Creates nucleotide sequence from specified {@code Bit2Array} (will be copied in constructor).
     *
     * @param data Bit2Array
     */
    public NucleotideSequence(Bit2Array data) {
        this.data = data.clone();
    }

    NucleotideSequence(Bit2Array data, boolean unsafe) {
        assert unsafe;
        this.data = data;
    }

    @Override
    public byte codeAt(int position) {
        return (byte) data.get(position);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public NucleotideSequence getRange(int from, int to) {
        return getRange(new Range(from, to));
    }

    @Override
    public NucleotideSequence getRange(Range range) {
        if (range.getLower() < 0 || range.getUpper() < 0
                || range.getLower() > size() || range.getUpper() > size())
            throw new IndexOutOfBoundsException();

        if (range.length() == 0)
            return EMPTY;

        if (range.isReverse())
            return new NucleotideSequence(
                    transformToRC(data, range.getLower(), range.getUpper()), true);
        else
            return new NucleotideSequence(
                    data.getRange(range.getLower(), range.getUpper()), true);
    }

    /**
     * Returns reverse complement of this sequence.
     *
     * @return reverse complement sequence
     */
    @Override
    public NucleotideSequence getReverseComplement() {
        return new NucleotideSequence(transformToRC(data, 0, data.size()), true);
    }

    /**
     * Returns a copy of inner data container.
     *
     * @return a copy of inner data container
     */
    public Bit2Array getInnerData() {
        return data.clone();
    }

    @Override
    public NucleotideAlphabet getAlphabet() {
        return ALPHABET;
    }

    @Override
    public int hashCode() {
        int result = ALPHABET.hashCode();
        result = 31 * result + data.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return data.equals(((NucleotideSequence) o).data);
    }

    /**
     * Creates nucleotide sequence from specified byte array.
     *
     * @param sequence byte array
     * @param offset   offset in {@code sequence}
     * @param length   length of resulting seqeunce
     * @return nucleotide sequence
     */
    public static NucleotideSequence fromSequence(byte[] sequence, int offset, int length) {
        Bit2Array storage = new Bit2Array(length);
        for (int i = 0; i < length; ++i)
            storage.set(i, ALPHABET.codeFromSymbol((char) sequence[offset + i]));
        return new NucleotideSequence(storage, true);
    }

    private static Bit2Array transformToRC(Bit2Array data, int from, int to) {
        Bit2Array newData = new Bit2Array(to - from);
        int reverseCord;
        for (int cord = 0, s = to - from; cord < s; ++cord) {
            reverseCord = to - 1 - cord;
            newData.set(cord, (~data.get(reverseCord)) & 0x3);
        }
        return newData;
    }
}
