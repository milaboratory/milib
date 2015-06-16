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
import com.milaboratory.primitivio.annotations.Serializable;

import java.io.ObjectStreamException;

/**
 * Interface for sequence letters alphabet (amino acid, nucleotide, etc.). {@code Alphabet} is responsible for
 * correspondence between char representation of elements (e.g. 'A', 'T', 'G', 'C' in case of
 * {@link com.milaboratory.core.sequence.NucleotideAlphabet}) and their internal byte representation.
 * <p>Implementation note: all alphabets should be singletons.</p>
 *
 * @param <S> type of sequences that correspond to this alphabet
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.Sequence
 * @see com.milaboratory.core.sequence.SequenceBuilder
 * @see com.milaboratory.core.sequence.NucleotideAlphabet
 * @see com.milaboratory.core.sequence.NucleotideSequence
 */
@JsonSerialize(using = Alphabets.Serializer.class)
@JsonDeserialize(using = Alphabets.Deserializer.class)
@Serializable(by = IO.AlphabetSerializer.class)
public abstract class Alphabet<S extends Sequence<S>> implements java.io.Serializable {
    private final String alphabetName;
    private final byte id;

    protected Alphabet(String alphabetName, byte id) {
        this.alphabetName = alphabetName;
        this.id = id;
    }

    /**
     * Gets a char from binary code
     *
     * @param code binary code of segment
     * @return corresponding char
     */
    public abstract char symbolFromCode(byte code);

    /**
     * Gets the number of letters in this alphabet
     *
     * @return the number of letters in this alphabet
     */
    public abstract int size();

    /**
     * Gets the binary code corresponding to given symbol (case insensitive) or -1 if there
     * is no such symbol in this alphabet
     *
     * @param symbol symbol to convert
     * @return binary code of the symbol (case insensitive) or -1 if there is no such symbol in the alphabet
     */
    public abstract byte codeFromSymbol(char symbol);

    /**
     * Returns a sequence builder for corresponding sequence type.
     *
     * @return sequence builder for corresponding sequence type
     */
    public abstract SequenceBuilder<S> getBuilder();

    /**
     * Returns the human readable name of this alphabet.
     *
     * <p>This name can be then used to obtain the instance of this alphabet using {@link
     * com.milaboratory.core.sequence.Alphabets#getByName(String)} method if it is registered (see {@link
     * com.milaboratory.core.sequence.Alphabets#register(Alphabet)}).</p>
     */
    public final String getAlphabetName() {
        return alphabetName;
    }

    /**
     * Returns byte id of this alphabet
     *
     * <p>This name can be then used to obtain the instance of this alphabet using {@link
     * com.milaboratory.core.sequence.Alphabets#getById(byte)} method if it is registered (see {@link
     * com.milaboratory.core.sequence.Alphabets#register(Alphabet)}).</p>
     */
    public byte getId() {
        return id;
    }

    /**
     * Parses string representation of sequence.
     *
     * @param string string representation of sequence
     * @return sequence
     */
    public S parse(String string) {
        SequenceBuilder<S> builder = getBuilder().ensureCapacity(string.length());
        for (int i = 0; i < string.length(); ++i)
            builder.append(codeFromSymbol(string.charAt(i)));
        return builder.createAndDestroy();
    }

    /**
     * Convert alphabet to a readable string.
     *
     * @return alphabet as a readable string
     */
    @Override
    public String toString() {
        return "Alphabet{" + alphabetName + '}';
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this;
    }

    protected Object writeReplace() throws ObjectStreamException {
        return new AlphabetSerialization(id);
    }

    protected static class AlphabetSerialization implements java.io.Serializable {
        final byte id;

        public AlphabetSerialization() {
            this.id = 0;
        }

        public AlphabetSerialization(byte id) {
            this.id = id;
        }

        private Object readResolve()
                throws ObjectStreamException {
            return Alphabets.getById(id);
        }
    }
}
