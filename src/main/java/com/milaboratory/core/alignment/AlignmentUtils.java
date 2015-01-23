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

    /**
     * Calculates score of alignments.
     *
     * @param scoring       scoring
     * @param initialLength length of initial sequence (before mutations; upper line of alignment)
     * @param mutations     array of mutations
     * @return score of alignment
     */
    public static float calculateScore(LinearGapAlignmentScoring scoring, int initialLength, Mutations mutations) {
        if (!scoring.uniformMatchScore())
            throw new IllegalArgumentException("Scoring with non-uniform match score is not supported.");

        float matchScore = scoring.getScore((byte) 0, (byte) 0);
        float score = matchScore * initialLength;
        for (int i = 0; i < mutations.size(); ++i) {
            int mutation = mutations.getMutation(i);
            if (isDeletion(mutation) || isInsertion(mutation))
                score += scoring.getGapPenalty();
            else //Substitution
                score += scoring.getScore((byte) getFrom(mutation), (byte) getTo(mutation));
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
                                    " but seq[" + pointer + "]=" + initialSequence.charFromCodeAt(pointer));
                        sb1.append(Character.toLowerCase(initialSequence.charFromCodeAt(pointer++)));
                        sb2.append(Character.toLowerCase(alphabet.symbolFromCode((byte) (mut & LETTER_MASK))));
                        ++mutPointer;
                        break;
                    case RAW_MUTATION_TYPE_DELETION:
                        if (((mut >> FROM_OFFSET) & LETTER_MASK) != initialSequence.codeAt(pointer))
                            throw new IllegalArgumentException("Mutation = " + Mutation.toString(initialSequence.getAlphabet(), mut) +
                                    " but seq[" + pointer + "]=" + initialSequence.charFromCodeAt(pointer));
                        sb1.append(initialSequence.charFromCodeAt(pointer++));
                        sb2.append("-");
                        ++mutPointer;
                        break;
                    case RAW_MUTATION_TYPE_INSERTION:
                        sb1.append("-");
                        sb2.append(alphabet.symbolFromCode((byte) (mut & LETTER_MASK)));
                        ++mutPointer;
                        break;
                }
            else {
                sb1.append(initialSequence.charFromCodeAt(pointer));
                sb2.append(initialSequence.charFromCodeAt(pointer++));
            }
        }

        return sb1.toString() + "\n" + sb2.toString() + '\n';
    }

}
