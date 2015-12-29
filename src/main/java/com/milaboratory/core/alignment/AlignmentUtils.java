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

import com.milaboratory.core.mutations.Mutation;
import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;

import static com.milaboratory.core.mutations.Mutation.*;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class AlignmentUtils {
    private AlignmentUtils() {
    }

    public static int calculateScore(AlignmentScoring scoring, int initialLength, Mutations mutations) {
        if (scoring instanceof AffineGapAlignmentScoring)
            return calculateScore((AffineGapAlignmentScoring) scoring, initialLength, mutations);
        else if (scoring instanceof LinearGapAlignmentScoring)
            return calculateScore((LinearGapAlignmentScoring) scoring, initialLength, mutations);
        else
            throw new IllegalArgumentException("Unknown scoring type");
    }

    /**
     * Calculates score of alignments.
     *
     * @param scoring       scoring
     * @param initialLength length of initial sequence (before mutations; upper line of alignment)
     * @param mutations     array of mutations
     * @return score of alignment
     */
    public static int calculateScore(AffineGapAlignmentScoring scoring, int initialLength, Mutations mutations) {
        if (!scoring.uniformBasicMatchScore())
            throw new IllegalArgumentException("Scoring with non-uniform match score is not supported.");

        int matchScore = scoring.getScore((byte) 0, (byte) 0);
        int score = matchScore * initialLength;
        int prevMutation = -1;
        for (int i = 0; i < mutations.size(); ++i) {
            int mutation = mutations.getMutation(i);

            if (scoring.getAlphabet().isWildcard(getFrom(mutation)) || scoring.getAlphabet().isWildcard(getTo(mutation)))
                throw new IllegalArgumentException("Mutations with wildcards are not supported.");

            if (isDeletion(mutation)) {
                if (i > 0 && isDeletion(prevMutation) && getPosition(mutation) - 1 == getPosition(prevMutation))
                    score += scoring.getGapExtensionPenalty();
                else
                    score += scoring.getGapOpenPenalty();
                score -= matchScore;
            } else if (isInsertion(mutation)) {
                if (i > 0 && isInsertion(prevMutation) && getPosition(mutation) == getPosition(prevMutation))
                    score += scoring.getGapExtensionPenalty();
                else
                    score += scoring.getGapOpenPenalty();
            } else {//Substitution
                score += scoring.getScore(getFrom(mutation), getTo(mutation));
                score -= matchScore;
            }

            prevMutation = mutation;
        }
        return score;
    }

    /**
     * Calculates score of alignments.
     *
     * @param scoring       scoring
     * @param initialLength length of initial sequence (before mutations; upper line of alignment)
     * @param mutations     array of mutations
     * @return score of alignment
     */
    public static int calculateScore(LinearGapAlignmentScoring scoring, int initialLength, Mutations mutations) {
        if (!scoring.uniformBasicMatchScore())
            throw new IllegalArgumentException("Scoring with non-uniform match score is not supported.");

        int matchScore = scoring.getScore((byte) 0, (byte) 0);
        int score = matchScore * initialLength;
        for (int i = 0; i < mutations.size(); ++i) {
            int mutation = mutations.getMutation(i);

            if (scoring.getAlphabet().isWildcard(getFrom(mutation)) || scoring.getAlphabet().isWildcard(getTo(mutation)))
                throw new IllegalArgumentException("Mutations with wildcards are not supported.");

            if (isDeletion(mutation) || isInsertion(mutation))
                score += scoring.getGapPenalty();
            else //Substitution
                score += scoring.getScore(getFrom(mutation), getTo(mutation));
            if (isDeletion(mutation) || isSubstitution(mutation))
                score -= matchScore;
        }
        return score;
    }

    public static <S extends Sequence<S>> String toStringSimple(S initialSequence, Mutations<S> mutations) {
        int pointer = 0;
        int mutPointer = 0;
        int mut;
        final Alphabet<S> alphabet = initialSequence.getAlphabet();
        StringBuilder sb1 = new StringBuilder(),
                sb2 = new StringBuilder();
        while (pointer < initialSequence.size() || mutPointer < mutations.size()) {
            if (mutPointer < mutations.size() && ((mut = mutations.getMutation(mutPointer)) >>> POSITION_OFFSET) <= pointer)
                switch (mut & MUTATION_TYPE_MASK) {
                    case RAW_MUTATION_TYPE_SUBSTITUTION:
                        if (((mut >> FROM_OFFSET) & LETTER_MASK) != initialSequence.codeAt(pointer))
                            throw new IllegalArgumentException("Mutation = " + Mutation.toString(initialSequence.getAlphabet(), mut) +
                                    " but seq[" + pointer + "]=" + initialSequence.symbolAt(pointer));
                        sb1.append(Character.toLowerCase(initialSequence.symbolAt(pointer++)));
                        sb2.append(Character.toLowerCase(alphabet.codeToSymbol((byte) (mut & LETTER_MASK))));
                        ++mutPointer;
                        break;
                    case RAW_MUTATION_TYPE_DELETION:
                        if (((mut >> FROM_OFFSET) & LETTER_MASK) != initialSequence.codeAt(pointer))
                            throw new IllegalArgumentException("Mutation = " + Mutation.toString(initialSequence.getAlphabet(), mut) +
                                    " but seq[" + pointer + "]=" + initialSequence.symbolAt(pointer));
                        sb1.append(initialSequence.symbolAt(pointer++));
                        sb2.append("-");
                        ++mutPointer;
                        break;
                    case RAW_MUTATION_TYPE_INSERTION:
                        sb1.append("-");
                        sb2.append(alphabet.codeToSymbol((byte) (mut & LETTER_MASK)));
                        ++mutPointer;
                        break;
                }
            else {
                sb1.append(initialSequence.symbolAt(pointer));
                sb2.append(initialSequence.symbolAt(pointer++));
            }
        }

        return sb1.toString() + "\n" + sb2.toString() + '\n';
    }

    public static <S extends Sequence<S>> S getAlignedSequence2Part(Alignment<S> alignment) {
        return alignment.getRelativeMutations().mutate(alignment.getSequence1().getRange(alignment.getSequence1Range()));
    }
}
