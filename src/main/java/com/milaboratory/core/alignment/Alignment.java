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
package com.milaboratory.core.alignment;

import com.milaboratory.core.Range;
import com.milaboratory.core.io.binary.AlignmentSerializer;
import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.primitivio.annotations.Serializable;
import com.milaboratory.util.BitArray;
import com.milaboratory.util.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import static com.milaboratory.core.mutations.Mutation.*;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
@Serializable(by = AlignmentSerializer.class)
public final class Alignment<S extends Sequence<S>> implements java.io.Serializable {
    /**
     * Initial sequence. (upper sequence in alignment; sequence1)
     */
    final S sequence1;
    /**
     * Mutations
     */
    final Mutations<S> mutations;
    /**
     * Range in initial sequence (sequence1)
     */
    final Range sequence1Range;
    /**
     * Range in mutated sequence (sequence2)
     */
    final Range sequence2Range;
    /**
     * Alignment score
     */
    final float score;

    public Alignment(S sequence1, Mutations<S> mutations, float score) {
        this(sequence1, mutations, new Range(0, sequence1.size()),
                new Range(0, sequence1.size() + mutations.getLengthDelta()),
                score);
    }

    public Alignment(S sequence1, Mutations<S> mutations, AlignmentScoring<S> scoring) {
        this(sequence1, mutations, new Range(0, sequence1.size()),
                new Range(0, sequence1.size() + mutations.getLengthDelta()),
                scoring);
    }

    public Alignment(S sequence1, Mutations<S> mutations,
                     Range sequence1Range, Range sequence2Range,
                     AlignmentScoring<S> scoring) {
        this(sequence1, mutations, sequence1Range, sequence2Range,
                AlignmentUtils.calculateScore(sequence1, sequence1Range, mutations, scoring));
    }

    public Alignment(S sequence1, Mutations<S> mutations,
                     Range sequence1Range, Range sequence2Range,
                     float score) {
        if (!mutations.isEmpty()) {
            if (!mutations.isCompatibleWith(sequence1)
                    || !sequence1Range.contains(mutations.getMutatedRange())
                    || sequence1Range.length() + mutations.getLengthDelta() != sequence2Range.length())
                throw new IllegalArgumentException("Not compatible arguments: muts: " + mutations + " range1: " + sequence1Range + " range2: " + sequence2Range);
        } else if (sequence1Range.length() != sequence2Range.length())
            throw new IllegalArgumentException("Not compatible arguments.");

        this.sequence1 = sequence1;
        this.mutations = mutations;
        this.sequence1Range = sequence1Range;
        this.sequence2Range = sequence2Range;
        this.score = score;
    }

    /**
     * Calculates score for this alignment using another scoring.
     *
     * @param scoring scoring
     * @return alignment score
     */
    public int calculateScore(AlignmentScoring<S> scoring) {
        return AlignmentUtils.calculateScore(sequence1, sequence1Range, mutations, scoring);
    }

    /**
     * Returns alignment iterator.
     *
     * @return alignment iterator
     */
    public AlignmentIterator<S> iterator() {
        return new AlignmentIterator<>(mutations, sequence1Range, sequence2Range.getFrom());
    }

    /**
     * Return initial sequence (upper sequence in alignment).
     *
     * @return initial sequence (upper sequence in alignment)
     */
    public S getSequence1() {
        return sequence1;
    }

    /**
     * Returns mutations in absolute (global) {@code sequence1} coordinates.
     *
     * @return mutations in absolute (global) {@code sequence1} coordinates
     */
    public Mutations<S> getAbsoluteMutations() {
        return mutations;
    }

    /**
     * Returns mutations in local coordinates, relative to {@code sequence1range}.
     *
     * @return mutations in local coordinates, relative to {@code sequence1range}
     */
    public Mutations<S> getRelativeMutations() {
        return mutations.move(-sequence1Range.getLower());
    }

    /**
     * Returns aligned range of sequence1.
     *
     * @return aligned range of sequence1
     */
    public Range getSequence1Range() {
        return sequence1Range;
    }

    /**
     * Returns aligned range of sequence2.
     *
     * @return aligned range of sequence2
     */
    public Range getSequence2Range() {
        return sequence2Range;
    }

    /**
     * Converts specified position from sequence1 coordinates to sequence2 coordinates. If position out of aligned
     * range of sequence1 range, returns -1. If letter at specified position in sequence1 is removed in sequence2, than
     * returns {@code -2 - p}, where {@code p} is a position of previous letter in sequence2.
     *
     * @param position position in sequence1
     * @return position in coordinates of sequence2, or -1 if specified position is out of aligned range of sequence1,
     * or if
     * letter at specified position in sequence1 is removed in sequence2  --- {@code -2 - p} where {@code p} is a
     * position of previous letter in sequence2
     */
    public int convertPosition(int position) {
        if (!sequence1Range.containsBoundary(position))
            return -1;
        int p = mutations.convertPosition(position);
        if (p < 0)
            return -2 - (~p + sequence2Range.getFrom() - sequence1Range.getFrom());
        return p + sequence2Range.getFrom() - sequence1Range.getFrom();
    }

    /**
     * Return alignment score
     *
     * @return alignment score
     */
    public float getScore() {
        return score;
    }

    /**
     * Having sequence2 creates alignment from sequence2 to sequence1
     *
     * @param sequence2 sequence2
     * @return inverted alignment
     */
    public Alignment<S> invert(S sequence2) {
        return new Alignment<>(sequence2, getRelativeMutations().invert().move(sequence2Range.getFrom()),
                sequence2Range, sequence1Range, score);
    }

    /**
     * Returns number of matches divided by sum of number of matches and mismatches.
     *
     * @return number of matches divided by sum of number of matches and mismatches
     */
    public float similarity() {
        int match = 0, mismatch = 0;

        AlignmentIterator<S> iterator = iterator();
        while (iterator.advance()) {
            final int mut = iterator.currentMutation;
            if (mut == NON_MUTATION)
                ++match;
            else
                ++mismatch;
        }

        return 1.0f * match / (match + mismatch);
    }

    /**
     * Returns alignment helper to simplify alignment output in conventional (BLAST) form.
     *
     * @return alignment helper
     */
    public AlignmentHelper getAlignmentHelper() {
        List<Boolean> matches = new ArrayList<>();

        IntArrayList pos1 = new IntArrayList(sequence1.size() + mutations.size()),
                pos2 = new IntArrayList(sequence1.size() + mutations.size());

        StringBuilder sb1 = new StringBuilder(),
                sb2 = new StringBuilder();

        AlignmentIterator<S> iterator = iterator();
        while (iterator.advance()) {
            final int mut = iterator.currentMutation;
            switch (getRawTypeCode(mut)) {
                case RAW_MUTATION_TYPE_SUBSTITUTION:
                    pos1.add(iterator.seq1Position);
                    pos2.add(iterator.seq2Position);
                    sb1.append(sequence1.symbolAt(iterator.seq1Position));
                    sb2.append(mutations.getToAsSymbolByIndex(iterator.mutationsPointer));
                    matches.add(false);
                    break;

                case RAW_MUTATION_TYPE_DELETION:
                    pos1.add(iterator.seq1Position);
                    pos2.add(-1 - iterator.seq2Position);
                    sb1.append(sequence1.symbolAt(iterator.seq1Position));
                    sb2.append("-");
                    matches.add(false);
                    break;

                case RAW_MUTATION_TYPE_INSERTION:
                    pos1.add(-1 - iterator.seq1Position);
                    pos2.add(iterator.seq2Position++);
                    sb1.append("-");
                    sb2.append(mutations.getToAsSymbolByIndex(iterator.mutationsPointer));
                    matches.add(false);
                    break;

                default:
                    pos1.add(iterator.seq1Position);
                    pos2.add(iterator.seq2Position);
                    sb1.append(sequence1.symbolAt(iterator.seq1Position));
                    sb2.append(sequence1.symbolAt(iterator.seq1Position));
                    matches.add(true);
                    break;
            }
        }

        return new AlignmentHelper(sb1.toString(), sb2.toString(),
                pos1.toArray(), pos2.toArray(),
                new BitArray(matches));
    }

    @Override
    public String toString() {
        return getAlignmentHelper().toCompactString();
    }

    public String toCompactString() {
        return "" + sequence1Range.getFrom() + "|" + sequence1Range.getTo() + "|" + sequence1.size() +
                "|" + sequence2Range.getFrom() + "|" + sequence2Range.getTo() + "|" + mutations.encode() + "|" + score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alignment alignment = (Alignment) o;

        if (Float.compare(alignment.score, score) != 0) return false;
        if (!mutations.equals(alignment.mutations)) return false;
        if (!sequence1.equals(alignment.sequence1)) return false;
        if (!sequence1Range.equals(alignment.sequence1Range)) return false;
        if (!sequence2Range.equals(alignment.sequence2Range)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sequence1.hashCode();
        result = 31 * result + mutations.hashCode();
        result = 31 * result + sequence1Range.hashCode();
        result = 31 * result + sequence2Range.hashCode();
        result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
        return result;
    }

    public static int aabs(int position) {
        if (position == -1)
            return -1;
        if (position < 0)
            return -2 - position;
        return position;
    }
}
