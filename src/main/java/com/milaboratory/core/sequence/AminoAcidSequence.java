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
 * Representation of amino acid sequences. Methods for translating nucleotide to amino acid and vice versa are placed
 * in {@link GeneticCode}
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.Sequence
 * @see com.milaboratory.core.sequence.AminoAcidAlphabet
 * @see GeneticCode
 */
public final class AminoAcidSequence extends AbstractArraySequence<AminoAcidSequence> {
    /**
     * Empty sequence
     */
    public static final AminoAcidSequence EMPTY = new AminoAcidSequence(new byte[0], true);
    /**
     * Amino acid alphabet
     */
    public static final AminoAcidAlphabet ALPHABET = AminoAcidAlphabet.INSTANCE;

    /**
     * Creates sequence with specified data.
     *
     * @param data byte array of amino acid codes from alphabet
     */
    public AminoAcidSequence(byte[] data) {
        super(data.clone());
    }

    /**
     * Creates amino acid sequence from its string representation (case insensitive).
     *
     * @param sequence string representation of amino acid sequence (case insensitive)
     * @throws java.lang.IllegalArgumentException if sequence contains unknown amino acid symbol
     */
    public AminoAcidSequence(String sequence) {
        super(sequence);
    }

    AminoAcidSequence(byte[] data, boolean unsafe) {
        super(data);
        assert unsafe;
    }

    @Override
    public AminoAcidAlphabet getAlphabet() {
        return ALPHABET;
    }

    /**
     * Returns whether this sequence contains stop codons
     *
     * @return whether this sequence contains stop codons
     */
    public boolean containStops() {
        for (byte b : data)
            if (b == AminoAcidAlphabet.STOP)
                return true;
        return false;
    }

    /**
     * Returns the number of stop codons that contained in this sequence
     *
     * @return number of stop codons that contained in this sequence
     */
    public int numberOfStops() {
        int count = 0;
        for (byte b : data)
            if (b == AminoAcidAlphabet.STOP)
                ++count;
        return count;
    }

    /**
     * Extracts {@literal int} representation of triplet starting from specified position (see implementation for
     * details).
     *
     * @param nSequence    nucleotide sequence
     * @param tripletStart position of first nucleotide of triplet
     * @return {@literal int} representation of triplet
     */
    public static int getTriplet(NucleotideSequence nSequence, int tripletStart) {
        int triplet = (nSequence.codeAt(tripletStart) << 4) |
                (nSequence.codeAt(tripletStart + 1) << 2) |
                nSequence.codeAt(tripletStart + 2);
        return triplet;
    }

    /**
     * Returns amino acid encoded by triplet starting from specified position (in terms of standard genetic code)
     *
     * @param nSequence    nucleotide sequence
     * @param tripletStart position of first nucleotide of triplet
     * @return byte-code of encoded amino acid
     */
    public static byte getAminoAcid(NucleotideSequence nSequence, int tripletStart) {
        return GeneticCode.getAminoAcid(getTriplet(nSequence, tripletStart));
    }

    /**
     * Translate sequence in one of frames (-1, -2, -3 frames are not implemented, use {@link
     * NucleotideSequence#getReverseComplement()}) discarding all incomplete codons on both boundaries.
     *
     * @param sequence nucleotide sequence to translate
     * @param frame    frame (1, 2 or 3)
     * @return translated amino acid sequence
     */
    public static AminoAcidSequence translate(NucleotideSequence sequence, int frame) {
        return translate(sequence.getRange(frame, frame + (sequence.size() - frame) / 3 * 3));
    }

    /**
     * Translates sequence having length divisible by 3, starting from first nucleotide.
     *
     * @param sequence nucleotide sequence
     * @return translated amino acid sequence
     */
    public static AminoAcidSequence translate(NucleotideSequence sequence) {
        if (sequence.size() % 3 != 0)
            throw new IllegalArgumentException("Only nucleotide sequences with size multiple " +
                    "of three are supported (in-frame).");
        byte[] aaData = new byte[sequence.size() / 3];
        GeneticCode.translate(aaData, 0, sequence, 0, sequence.size());
        return new AminoAcidSequence(aaData, true);
    }

    /**
     * Use one of specialized method instead:
     *
     * <ul>
     * <li>{@link #convertPositionFromLeft(int, int)}</li>
     * <li>{@link #convertPositionFromRight(int, int)}</li>
     * <li>{@link #convertPositionFromCenter(int, int)}</li>
     * </ul>
     */
    public static AminoAcidSequencePosition convertPosition(int ntPosition, Boolean fromLeft, boolean includeIncomplete,
                                                            int ntSequenceLength) {
        int aaSequenceSize = (ntSequenceLength + 2) / 3;
        if (fromLeft == null) {
            if (!includeIncomplete)
                throw new IllegalArgumentException("Illegal argument combination: includeIncomplete=false & fromLeft=null .");
            int aaLength = ntSequenceLength / 3;
            int leftAALength = (aaLength + 1) / 2;
            int rightAALength = aaLength - leftAALength;
            // Next position after last nucleotide in left part of sequence
            int lastLeftNt = ntSequenceLength - rightAALength * 3;
            return ntPosition < lastLeftNt ? convertPosition(ntPosition, true, true, ntSequenceLength) :
                    convertPosition(ntPosition, false, true, ntSequenceLength);
        } else if (fromLeft) {
            int aa = ntPosition / 3;
            return new AminoAcidSequencePosition(aa, ntPosition % 3);
        } else {
            ntPosition -= (ntSequenceLength % 3);
            if ((ntSequenceLength % 3) != 0)
                ntPosition += 3;
            return new AminoAcidSequencePosition(ntPosition / 3, ntPosition % 3);
        }
    }

    /**
     * Converts position from nucleotide to amino acid sequence if it was translated using
     * {@link #translateFromRight(NucleotideSequence)}.
     *
     * @param ntPosition       position in nucleotide sequence
     * @param ntSequenceLength length of nucleotide sequence
     * @return position in amino acid sequence
     */
    public static AminoAcidSequencePosition convertPositionFromRight(int ntPosition, int ntSequenceLength) {
        return convertPosition(ntPosition, false, true, ntSequenceLength);
    }

    /**
     * Converts position from nucleotide to amino acid sequence if it was translated using
     * {@link #translateFromLeft(NucleotideSequence)}.
     *
     * @param ntPosition       position in nucleotide sequence
     * @param ntSequenceLength length of nucleotide sequence
     * @return position in amino acid sequence
     */
    public static AminoAcidSequencePosition convertPositionFromLeft(int ntPosition, int ntSequenceLength) {
        return convertPosition(ntPosition, true, true, ntSequenceLength);
    }

    /**
     * Converts position from nucleotide to amino acid sequence if it was translated using
     * {@link #translateFromCenter(NucleotideSequence)}.
     *
     * @param ntPosition       position in nucleotide sequence
     * @param ntSequenceLength length of nucleotide sequence
     * @return position in amino acid sequence
     */
    public static AminoAcidSequencePosition convertPositionFromCenter(int ntPosition, int ntSequenceLength) {
        return convertPosition(ntPosition, null, true, ntSequenceLength);
    }

    /**
     * Use one of specialized method instead:
     *
     * <ul>
     * <li>{@link #translateFromLeft(NucleotideSequence)}</li>
     * <li>{@link #translateFromRight(NucleotideSequence)}</li>
     * <li>{@link #translateFromCenter(NucleotideSequence)}</li>
     * </ul>
     */
    public static AminoAcidSequence translate(Boolean fromLeft, boolean includeIncomplete, NucleotideSequence ns) {
        byte[] data;
        data = new byte[(ns.size() + (includeIncomplete ? 2 : 0)) / 3];
        if (fromLeft == null) {
            if (!includeIncomplete)
                throw new IllegalArgumentException("Illegal argument combination: includeIncomplete=false & fromLeft=null .");
            int aaLength = ns.size() / 3;
            int leftAALength = (aaLength + 1) / 2;
            int rightAALength = aaLength - leftAALength;
            GeneticCode.translate(data, 0, ns, 0, leftAALength * 3);
            GeneticCode.translate(data, data.length - rightAALength, ns, ns.size() - rightAALength * 3, rightAALength * 3);
            if (ns.size() % 3 != 0)
                data[leftAALength] = AminoAcidAlphabet.INCOMPLETE_CODON;
        } else if (fromLeft) {
            GeneticCode.translate(data, 0, ns, 0, ns.size() / 3 * 3);
            if (includeIncomplete && ns.size() % 3 != 0)
                data[data.length - 1] = AminoAcidAlphabet.INCOMPLETE_CODON;
        } else {
            if (includeIncomplete && ns.size() % 3 != 0)
                data[0] = AminoAcidAlphabet.INCOMPLETE_CODON;
            GeneticCode.translate(data,
                    (includeIncomplete && ns.size() % 3 != 0) ? 1 : 0, ns, ns.size() % 3, ns.size() / 3 * 3);
        }
        return new AminoAcidSequence(data, true);
    }

    /**
     * Translates sequence from the right side, so the last (3rd) nucleotide of last triplet matches last nucleotide of
     * the sequence. Incomplete codon added at the first position of resulting amino acid sequence if initial
     * nucleotide
     * sequence length is not divisible by 3.
     *
     * <pre>
     *
     * Example for sequence: ATGTCACA
     *
     *  AT GTC ACA
     *  _   V   T
     * </pre>
     *
     * @param ns nucleotide sequence to translate
     * @return result of translation (see description)
     */
    public static AminoAcidSequence translateFromRight(NucleotideSequence ns) {
        return translate(false, true, ns);
    }

    /**
     * Translates sequence from the left side, so the first nucleotide of the first triplet matches first nucleotide of
     * the sequence. Incomplete codon added at the last position of resulting amino acid sequence if initial nucleotide
     * sequence length is not divisible by 3.
     *
     * <pre>
     *
     * Example for sequence: ATTAGACA
     *
     *  ATT AGA CA
     *   I   R   _
     * </pre>
     *
     * @param ns nucleotide sequence to translate
     * @return result of translation (see description)
     */
    public static AminoAcidSequence translateFromLeft(NucleotideSequence ns) {
        return translate(true, true, ns);
    }

    /**
     * Translates sequence from both sides, so the first nucleotide of the first triplet matches first nucleotide of
     * the sequence, and last (3rd) nucleotide of last triplet matches last nucleotide of the sequence. Incomplete
     * codon added in the middle of resulting amino acid sequence if initial nucleotide sequence length is not
     * divisible by 3. This method is useful for CDR3 translation? as it preserves original aa sequences of germline
     * V/J
     * genes in case of out-of-frame assemblies.
     *
     * <pre>
     *
     * Example for sequence: ATTAGACA
     *
     *  ATT AG  ACA
     *   I   _   T
     * </pre>
     *
     * @param ns nucleotide sequence to translate
     * @return result of translation (see description)
     */
    public static AminoAcidSequence translateFromCenter(NucleotideSequence ns) {
        return translate(null, true, ns);
    }

    /**
     * This class represents mapping of nucleotide sequence position onto translated amino acid sequence.
     *
     * Use value of {@link #aminoAcidPosition} field, of use {@link #floor()} or {@link #ceil()} methods to get integer
     * value.
     */
    public static final class AminoAcidSequencePosition {
        /**
         * Position of amino acid in aa sequence
         */
        public final int aminoAcidPosition;
        /**
         * Position of particular nucleotide in triplet encoding amino acid at {@code aminoAcidPosition}.
         */
        public final byte positionInTriplet;

        public AminoAcidSequencePosition(int aminoAcidPosition, int positionInTriplet) {
            this.aminoAcidPosition = aminoAcidPosition;
            this.positionInTriplet = (byte) positionInTriplet;
        }

        public int floor() {
            return aminoAcidPosition;
        }

        public int ceil() {
            return positionInTriplet == 0 ? aminoAcidPosition : aminoAcidPosition + 1;
        }

        @Override
        public String toString() {
            return "A" + aminoAcidPosition + "+" + positionInTriplet + "n";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AminoAcidSequencePosition)) return false;

            AminoAcidSequencePosition that = (AminoAcidSequencePosition) o;

            if (aminoAcidPosition != that.aminoAcidPosition) return false;
            return positionInTriplet == that.positionInTriplet;
        }

        @Override
        public int hashCode() {
            int result = aminoAcidPosition;
            result = 31 * result + (int) positionInTriplet;
            return result;
        }
    }
}
