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
        super(alphabet.getAlphabetName() + "_incomplete");
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
