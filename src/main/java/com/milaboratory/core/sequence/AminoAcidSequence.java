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
 * Representation of amino acid sequences. Methods for translating nucleotide to amino acid and vice versa are placed in
 * {@link com.milaboratory.core.sequence.Translator}
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.Sequence
 * @see com.milaboratory.core.sequence.AminoAcidAlphabet
 * @see com.milaboratory.core.sequence.Translator
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
            if (b == AminoAcidAlphabet.Stop)
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
            if (b == AminoAcidAlphabet.Stop)
                ++count;
        return count;
    }

    public static int getTriplet(NucleotideSequence nSequence, int tripletStart) {
        int triplet = (nSequence.codeAt(tripletStart) << 4) | (nSequence.codeAt(tripletStart + 1) << 2) | nSequence.codeAt(tripletStart + 2);
        return triplet;
    }

    public static byte getAminoAcid(NucleotideSequence nSequence, int tripletStart) {
        return Translator.getAminoAcid(getTriplet(nSequence, tripletStart));
    }

    public static AminoAcidSequence translate(NucleotideSequence sequence) {
        if (sequence.size() % 3 != 0)
            throw new IllegalArgumentException("Only nucleotide sequences with size multiple " +
                    "of three are supported (in-frame).");
        byte[] aaData = new byte[sequence.size() / 3];
        Translator.translate(aaData, 0, sequence, 0, sequence.size());
        return new AminoAcidSequence(aaData, true);
    }

    public static AminoAcidSequencePosition convertPosition(int ntPosition, Boolean fromLeft, boolean includeIncomplete, int ntSequenceLength) {
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

    public static AminoAcidSequencePosition convertFromRight(int ntPosition, int ntSequenceLength) {
        return convertPosition(ntPosition, false, true, ntSequenceLength);
    }

    public static AminoAcidSequencePosition convertFromLeft(int ntPosition, int ntSequenceLength) {
        return convertPosition(ntPosition, true, true, ntSequenceLength);
    }

    public static AminoAcidSequencePosition convertFromCenter(int ntPosition, int ntSequenceLength) {
        return convertPosition(ntPosition, null, true, ntSequenceLength);
    }

    public static AminoAcidSequence translate(Boolean fromLeft, boolean includeIncomplete, NucleotideSequence ns) {
        byte[] data;
        data = new byte[(ns.size() + (includeIncomplete ? 2 : 0)) / 3];
        if (fromLeft == null) {
            if (!includeIncomplete)
                throw new IllegalArgumentException("Illegal argument combination: includeIncomplete=false & fromLeft=null .");
            int aaLength = ns.size() / 3;
            int leftAALength = (aaLength + 1) / 2;
            int rightAALength = aaLength - leftAALength;
            Translator.translate(data, 0, ns, 0, leftAALength * 3);
            Translator.translate(data, data.length - rightAALength, ns, ns.size() - rightAALength * 3, rightAALength * 3);
            if (ns.size() % 3 != 0)
                data[leftAALength] = AminoAcidAlphabet.IncompleteCodon;
        } else if (fromLeft) {
            Translator.translate(data, 0, ns, 0, ns.size() / 3 * 3);
            if (includeIncomplete && ns.size() % 3 != 0)
                data[data.length - 1] = AminoAcidAlphabet.IncompleteCodon;
        } else {
            if (includeIncomplete && ns.size() % 3 != 0)
                data[0] = AminoAcidAlphabet.IncompleteCodon;
            Translator.translate(data,
                    (includeIncomplete && ns.size() % 3 != 0) ? 1 : 0, ns, ns.size() % 3, ns.size() / 3 * 3);
        }
        return new AminoAcidSequence(data, true);
    }

    public static AminoAcidSequence translateFromRight(NucleotideSequence ns) {
        return translate(false, true, ns);
    }

    public static AminoAcidSequence translateFromLeft(NucleotideSequence ns) {
        return translate(true, true, ns);
    }

    public static AminoAcidSequence translateFromCenter(NucleotideSequence ns) {
        return translate(null, true, ns);
    }

    public static final class AminoAcidSequencePosition {
        public final int aminoAcidPosition;
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
