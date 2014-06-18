package com.milaboratory.core.sequence;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public abstract class IncompleteAlphabet<IS extends IncompleteSequence, S extends Sequence>
        extends AbstractArrayAlphabet<IS> {

    public static final IncompleteAlphabet<IncompleteNucleotideSequence, NucleotideSequence>
            INCOMPLETE_NUCLEOTIDE_ALPHABET =
            new IncompleteAlphabet<IncompleteNucleotideSequence, NucleotideSequence>(NucleotideAlphabet.INSTANCE) {
                @Override
                IncompleteNucleotideSequence createUnsafe(byte[] array) {
                    return new IncompleteNucleotideSequence(array, true);
                }
            };

    public static final IncompleteAlphabet<IncompleteAminoAcidSequence, AminoAcidSequence>
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
        this.unknownLetterCode = (byte) (alphabet.size() + 1);
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
        return unknownLetterCode;
    }

    public byte getUnknownLetterCode() {
        return unknownLetterCode;
    }

    public Alphabet<S> getOrigin() {
        return alphabet;
    }
}
