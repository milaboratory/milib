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
package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.core.sequence.SequenceBuilder;
import com.milaboratory.primitivio.annotations.Serializable;
import com.milaboratory.util.IntArrayList;

import java.util.Arrays;

import static com.milaboratory.core.mutations.Mutation.*;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
@Serializable(by = IO.MutationsSerializer.class)
public final class Mutations<S extends Sequence<S>> {
    final Alphabet<S> alphabet;
    final int[] mutations;

    public Mutations(Alphabet alphabet, IntArrayList mutations) {
        this(alphabet, mutations.toArray(), true);
    }

    public Mutations(Alphabet alphabet, int... mutations) {
        if (!MutationsUtil.isSorted(mutations))
            throw new IllegalArgumentException("Not sorted according to positions.");
        this.mutations = mutations.clone();
        this.alphabet = alphabet;
    }

    Mutations(Alphabet alphabet, int[] mutations, boolean unsafe) {
        assert unsafe;
        assert MutationsUtil.isSorted(mutations);
        this.mutations = mutations;
        this.alphabet = alphabet;
    }

    public int size() {
        return mutations.length;
    }

    public int getMutation(int index) {
        return mutations[index];
    }

    public int[] getAllMutations() {
        return mutations.clone();
    }

    public boolean isEmpty() {
        return mutations.length == 0;
    }

    public int getPositionByIndex(int index) {
        return getPosition(mutations[index]);
    }

    public byte getFromAsCodeByIndex(int index) {
        return getFrom(mutations[index]);
    }

    public byte getToAsCodeByIndex(int index) {
        return getTo(mutations[index]);
    }

    public char getFromAsSymbolByIndex(int index) {
        return alphabet.symbolFromCode(getFromAsCodeByIndex(index));
    }

    public char getToAsSymbolByIndex(int index) {
        return alphabet.symbolFromCode(getToAsCodeByIndex(index));
    }

    public int getRawTypeByIndex(int index) {
        return getRawTypeCode(mutations[index]);
    }

    public MutationType getTypeByIndex(int index) {
        return getType(mutations[index]);
    }

    public boolean isCompatibleWith(S sequence) {
        return MutationsUtil.isCompatibleWithSequence(sequence, mutations);
    }

    public S mutate(S sequence) {
        int length = sequence.size();
        for (int i : mutations)
            switch (i & MUTATION_TYPE_MASK) {
                case RAW_MUTATION_TYPE_DELETION:
                    --length;
                    break;
                case RAW_MUTATION_TYPE_INSERTION:
                    ++length;
                    break;
            }
        SequenceBuilder<S> builder = alphabet.getBuilder().ensureCapacity(length);
        int pointer = 0;
        int mutPointer = 0;
        int mut;
        while (pointer < sequence.size() || mutPointer < mutations.length) {
            if (mutPointer < mutations.length && ((mut = mutations[mutPointer]) >>> POSITION_OFFSET) <= pointer)
                switch (mut & MUTATION_TYPE_MASK) {
                    case RAW_MUTATION_TYPE_SUBSTITUTION:
                        if (((mut >> FROM_OFFSET) & LETTER_MASK) != sequence.codeAt(pointer))
                            throw new IllegalArgumentException("Mutation = " + Mutation.toString(sequence.getAlphabet(), mut) +
                                    " but seq[" + pointer + "]=" + sequence.charFromCodeAt(pointer));

                        ++pointer;
                        builder.append((byte) (mut & LETTER_MASK));
                        ++mutPointer;
                        break;
                    case RAW_MUTATION_TYPE_DELETION:
                        if (((mut >> FROM_OFFSET) & LETTER_MASK) != sequence.codeAt(pointer))
                            throw new IllegalArgumentException("Mutation = " + Mutation.toString(sequence.getAlphabet(), mut) +
                                    " but seq[" + pointer + "]=" + sequence.charFromCodeAt(pointer));

                        ++pointer;
                        ++mutPointer;
                        break;
                    case RAW_MUTATION_TYPE_INSERTION:
                        builder.append((byte) (mut & LETTER_MASK));
                        ++mutPointer;
                        break;
                }
            else
                builder.append(sequence.codeAt(pointer++));
        }
        return builder.createAndDestroy();
    }

    /**
     * Converts position from coordinates in seq1 to coordinates in seq2 using this alignment (mutations). <p/> <p>If
     * letter in provided position is marked as deleted (deletion) in this mutations, this method will return {@code (-
     * 1 - imagePosition)}, where {@code imagePosition} is a position of letter right after that place where target
     * nucleotide was removed according to this alignment.</p>
     *
     * @param initialPosition position in seq1
     * @return converted position
     */
    public int convertPosition(int initialPosition) {
        int p, result = initialPosition;

        for (int mut : mutations) {
            p = getPosition(mut);

            if (p > initialPosition)
                return result;

            switch (mut & MUTATION_TYPE_MASK) {
                case RAW_MUTATION_TYPE_DELETION:
                    if (p == initialPosition)
                        return -result - 1;
                    --result;
                    break;
                case RAW_MUTATION_TYPE_INSERTION:
                    ++result;
                    break;
            }
        }

        return result;
    }

    /**
     * Returns the difference between the length of initial sequence and length of mutated sequence. Negative values
     * denotes that mutated sequence is shorter.
     *
     * @return difference between the length of initial sequence and mutated sequence
     */
    public int getLengthDelta() {
        int delta = 0;

        for (int mut : mutations)
            switch (mut & MUTATION_TYPE_MASK) {
                case RAW_MUTATION_TYPE_DELETION:
                    --delta;
                    break;
                case RAW_MUTATION_TYPE_INSERTION:
                    ++delta;
                    break;
            }

        return delta;
    }

    /**
     * Returns combined mutations array ({@code this} applied before {@code other}).
     *
     * @param other second mutations object
     * @return combined mutations
     */
    public Mutations<S> combineWith(final Mutations<S> other) {
        IntArrayList result = new IntArrayList(mutations.length + other.mutations.length);

        //mut2 pointer
        int p2 = 0, position0 = 0, delta = 0;

        for (int p1 = 0; p1 < mutations.length; ++p1) {

            position0 = getPosition(mutations[p1]);

            while (p2 < other.mutations.length && // There are mutations in m2
                    (getPosition(other.mutations[p2]) < position0 + delta || // Before current point
                            (getPosition(other.mutations[p2]) == position0 + delta && // On the current point and it is insertion
                                    getRawTypeCode(other.mutations[p2]) == RAW_MUTATION_TYPE_INSERTION)))
                appendInCombine(result, Mutation.move(other.mutations[p2++], -delta));

            switch (getRawTypeCode(mutations[p1])) {
                case RAW_MUTATION_TYPE_INSERTION:
                    if (p2 < other.mutations.length && getPosition(other.mutations[p2]) == delta + position0) {
                        if (getTo(mutations[p1]) != getFrom(other.mutations[p2]))
                            throw new IllegalArgumentException();

                        if (isSubstitution(other.mutations[p2]))
                            appendInCombine(result, (mutations[p1] & (~LETTER_MASK)) | (other.mutations[p2] & LETTER_MASK));

                        ++p2;
                    } else
                        appendInCombine(result, mutations[p1]);
                    ++delta;
                    break;
                case RAW_MUTATION_TYPE_SUBSTITUTION:
                    if (p2 < other.mutations.length && getPosition(other.mutations[p2]) == delta + position0) {

                        if (getTo(mutations[p1]) != getFrom(other.mutations[p2]))
                            throw new IllegalArgumentException();

                        if (isSubstitution(other.mutations[p2]))
                            appendInCombine(result, (mutations[p1] & (~LETTER_MASK)) | (other.mutations[p2] & LETTER_MASK));
                        else if (isDeletion(other.mutations[p2]))
                            appendInCombine(result, createDeletion(position0, getFrom(mutations[p1])));
                        else
                            throw new RuntimeException("Insertion after Del. or Subs.");

                        ++p2;

                    } else
                        appendInCombine(result, mutations[p1]);

                    break;
                case RAW_MUTATION_TYPE_DELETION:
                    --delta;
                    appendInCombine(result, mutations[p1]);
                    break;
            }

        }

        while (p2 < other.mutations.length)
            appendInCombine(result, Mutation.move(other.mutations[p2++], -delta));

        return new Mutations<S>(alphabet, result.toArray(), true);
    }

    /**
     * Moves positions of mutations by specified offset
     *
     * @param offset offset
     * @return relocated positions
     */
    public Mutations<S> move(int offset) {
        int[] newMutations = new int[mutations.length];
        for (int i = 0; i < mutations.length; ++i)
            newMutations[i] = Mutation.move(mutations[i], offset);
        return new Mutations<S>(alphabet, newMutations, true);
    }

    /**
     * Extracts mutations for a range of positions in the original sequence and performs shift of corresponding
     * positions (moves them to {@code -range.from}). <p/> <p>Insertions before {@code range.from} excluded. Insertions
     * after {@code (range.to - 1)} included.</p> <p/> <p><b>Important:</b> to extract leftmost insertions (trailing
     * insertions) use {@code range.from = -1}.</p>
     *
     * @param range range
     * @return mutations for a range of positions
     */
    public Mutations<S> extractMutationsForRange(Range range) {
        if (range.isReverse())
            throw new IllegalArgumentException("Reverse ranges are not supported by this method.");

        return extractMutationsForRange(range.getFrom(), range.getTo());
    }

    /**
     * Extracts mutations for a range of positions in the original sequence and performs shift of corresponding
     * positions (moves them to {@code -from}). <p/> <p>Insertions before {@code from} excluded. Insertions after {@code
     * (to - 1)} included.</p> <p/> <p><b>Important:</b> to extract leftmost insertions (trailing insertions) use {@code
     * from = -1}. So {@code extractMutationsForRange(mut, -1, seqLength) == mut}.</p>
     *
     * @param from left bound of range, inclusive. Use -1 to extract leftmost insertions.
     * @param to   right bound of range, exclusive
     * @return mutations for a range of positions
     */
    public Mutations<S> extractMutationsForRange(int from, int to) {
        //If range size is 0 return empty array
        if (from == to)
            return new Mutations<S>(alphabet, new int[0], true);

        //Find first mutation for the range
        int fromIndex = firstMutationWithPosition(from);
        if (fromIndex < 0)
            fromIndex = -fromIndex - 1;

        // If first mutations are insertions with position == from:
        // remove them from output
        while (fromIndex < mutations.length &&
                (mutations[fromIndex] >>> POSITION_OFFSET) == from &&
                (mutations[fromIndex] & MUTATION_TYPE_MASK) == RAW_MUTATION_TYPE_INSERTION)
            ++fromIndex;

        //Find last mutation
        int toIndex = firstMutationWithPosition(fromIndex, mutations.length, to);
        if (toIndex < 0)
            toIndex = -toIndex - 1;

        while (toIndex < mutations.length &&
                (mutations[toIndex] >>> POSITION_OFFSET) == to &&
                (mutations[toIndex] & MUTATION_TYPE_MASK) == RAW_MUTATION_TYPE_INSERTION)
            ++toIndex;

        //Creating result
        int[] result = new int[toIndex - fromIndex];

        //Constant to move positions in the output array
        int offset;
        if (from == -1)
            offset = 0;
        else
            offset = ((-from) << POSITION_OFFSET);

        //Copy and move mutations
        for (int i = result.length - 1, j = toIndex - 1; i >= 0; --i, --j)
            result[i] = mutations[j] + offset;

        return new Mutations<S>(alphabet, result, true);
    }

    /**
     * Inverts mutations, so that they reflect difference from seq2 to seq1. <p/> E.g. for mutations generated with
     * <pre>
     * NucleotideSequence ref = randomSequence(300);
     * int[] mutations = Mutations.generateMutations(ref,
     *                             MutationModels.getEmpiricalNucleotideMutationModel()
     *                             .multiply(3.0));
     * </pre>
     * and the inverted mutations
     * <pre>
     * int[] invMutations = ConsensusAligner.invertMutations(mutations);
     * </pre>
     * The following two methods are equal
     * <pre>
     * Mutations.printAlignment(ref, mutations);
     * Mutations.printAlignment(Mutations.mutate(ref, mutations), invMutations);
     * </pre>
     * Same stands for
     * <pre>
     * Mutations.getPosition(mutations, posInSeq1)
     * </pre>
     *
     * @return mutations that will generate seq1 from seq2
     */
    public Mutations<S> invert() {
        int[] newMutations = new int[mutations.length];
        int delta = 0;
        for (int i = 0; i < mutations.length; i++) {
            int from = getFrom(mutations[i]);
            int to = getTo(mutations[i]);
            int pos = getPosition(mutations[i]);
            int type = getRawTypeCode(mutations[i]);
            switch (type) {
                case RAW_MUTATION_TYPE_DELETION:
                    delta--;
                    type = RAW_MUTATION_TYPE_INSERTION;
                    pos++;
                    break;
                case RAW_MUTATION_TYPE_INSERTION:
                    delta++;
                    type = RAW_MUTATION_TYPE_DELETION;
                    pos--;
                    break;
                default:
                    break;
            }
            newMutations[i] = createMutation(type, pos + delta, to, from);
        }
        return new Mutations<S>(alphabet, newMutations, true);
    }

    public int countOfIndels() {
        int result = 0;
        for (int mutation : mutations)
            switch (mutation & MUTATION_TYPE_MASK) {
                case RAW_MUTATION_TYPE_DELETION:
                case RAW_MUTATION_TYPE_INSERTION:
                    result++;
            }

        return result;
    }

    public int countOf(final MutationType type) {
        int result = 0;
        for (int mutation : mutations)
            if ((mutation & MUTATION_TYPE_MASK) == type.rawType)
                result++;
        return result;
    }

    /**
     * Extracts sub mutations by {@code from}-{@code to} mutation indices.
     *
     * @param from index in current mutations object pointing to the first mutation to be extracted
     * @param to   index in current mutations object pointing to the next after last mutation to be extracted
     * @return sub mutations
     */
    public Mutations<S> getRange(int from, int to) {
        return new Mutations<S>(alphabet, Arrays.copyOfRange(mutations, from, to));
    }

    public int firsMutationPosition() {
        if (isEmpty())
            return -1;

        return getPosition(mutations[0]);
    }

    public int lastMutationPosition() {
        if (isEmpty())
            return -1;

        return getPosition(mutations[mutations.length - 1]);
    }

    public Range getMutatedRange() {
        if (isEmpty())
            return null;
        return new Range(firsMutationPosition(), lastMutationPosition());
    }

    @Override
    public String toString() {
        if (mutations.length == 0)
            return "[]";

        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int mut : mutations)
            builder.append(Mutation.toString(alphabet, mut) + ",");
        builder.deleteCharAt(builder.length() - 1);
        builder.append("]");
        return builder.toString();
    }

    public String encode() {
        return MutationsUtil.encode(mutations, alphabet);
    }

    public static <S extends Sequence<S>> Mutations<S> decode(String string, Alphabet<S> alphabet) {
        return new Mutations<S>(alphabet, MutationsUtil.decode(string, alphabet), true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mutations mutations1 = (Mutations) o;
        if (alphabet != mutations1.alphabet) return false;
        return Arrays.equals(mutations, mutations1.mutations);
    }

    @Override
    public int hashCode() {
        int result = alphabet.hashCode();
        result = 31 * result + Arrays.hashCode(mutations);
        return result;
    }

    public int firstMutationWithPosition(int position) {
        return firstMutationWithPosition(0, mutations.length, position);
    }

    public int firstMutationWithPosition(int fromIndex, int toIndex, int position) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = mutations[mid] >>> POSITION_OFFSET;

            if (midVal < position)
                low = mid + 1;
            else if (midVal > position)
                high = mid - 1;
            else {
                // key found
                // searching for first occurance
                while (mid > 0 && (mutations[mid - 1] >>> POSITION_OFFSET) == position)
                    --mid;

                return mid;
            }
        }
        return -(low + 1);  // key not found.
    }

    private static void appendInCombine(IntArrayList result, int mutation) {
        if (isSubstitution(mutation) || result.isEmpty())
            result.add(mutation);
        else {
            int last = result.peek();

            if (isSubstitution(last))
                result.add(mutation);
            else {

                int lPosition = getPosition(last);
                int mPosition = getPosition(mutation);

                if (lPosition == mPosition &&
                        isInsertion(last) && isDeletion(mutation))
                    cfs(result, lPosition, getFrom(mutation), getTo(last));
                else if (lPosition == mPosition - 1 &&
                        isDeletion(last) && isInsertion(mutation))
                    cfs(result, lPosition, getFrom(last), getTo(mutation));
                else
                    result.add(mutation);
            }
        }
    }

    private static void cfs(IntArrayList result, int position, int from, int to) {
        if (from == to)
            result.pop();
        else
            result.set(result.size() - 1, createSubstitution(position, from, to));
    }
}
