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
 * Alphabet fot incomplete sequences.
 *
 * @param <IS> type of incomplete sequence
 * @param <S>  type of sequence for incomplete sequence is defined: for example, if IS is
 *             {@code IncompleteNucleotideSequence}, then S is {@code NucleotideSequence}
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 * @see com.milaboratory.core.sequence.Alphabet
 * @see com.milaboratory.core.sequence.IncompleteSequence
 */
public abstract class IncompleteAlphabet<IS extends IncompleteSequence<IS, S>, S extends Sequence<S>>
        extends AbstractArrayAlphabet<IS> {

    static final IncompleteAlphabet<IncompleteNucleotideSequence, NucleotideSequence>
            INCOMPLETE_NUCLEOTIDE_ALPHABET =
            new IncompleteAlphabet<IncompleteNucleotideSequence, NucleotideSequence>(NucleotideAlphabet.INSTANCE) {
                @Override
                IncompleteNucleotideSequence createUnsafe(byte[] array) {
                    return new IncompleteNucleotideSequence(array, true);
                }
            };

    static final IncompleteAlphabet<IncompleteAminoAcidSequence, AminoAcidSequence>
            INCOMPLETE_AMINO_ACID_ALPHABET =
            new IncompleteAlphabet<IncompleteAminoAcidSequence, AminoAcidSequence>(AminoAcidAlphabet.INSTANCE) {
                @Override
                IncompleteAminoAcidSequence createUnsafe(byte[] array) {
                    return new IncompleteAminoAcidSequence(array, true);
                }
            };

    final Alphabet<S> alphabet;
    final byte unknownLetterCode;
    public static final char UNKNOWN_LETTER = '.';

    private IncompleteAlphabet(Alphabet<S> alphabet) {
        super(alphabet.getAlphabetName() + "_incomplete", (byte) (alphabet.getId() + 64));
        this.alphabet = alphabet;
        this.unknownLetterCode = (byte) alphabet.size();
    }

    @Override
    public char symbolFromCode(byte code) {
        return code == unknownLetterCode ? UNKNOWN_LETTER : alphabet.symbolFromCode(code);
    }

    @Override
    public byte codeFromSymbol(char symbol) {
        byte b = alphabet.codeFromSymbol(symbol);
        return b == -1 ? unknownLetterCode : b;
    }

    @Override
    public int size() {
        return unknownLetterCode + 1;
    }

    /**
     * Returns binary code for unknown letter.
     *
     * @return binary code for unknown letter.
     */
    public byte getUnknownLetterCode() {
        return unknownLetterCode;
    }

    /**
     * Returns an alphabet for parent type of sequence.
     *
     * @return alphabet for parent type of sequence
     */
    public Alphabet<S> getOrigin() {
        return alphabet;
    }
}
