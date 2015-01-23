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

import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for sequences.
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 */
public final class SequencesUtils {
    /**
     * Check if a sequence contains letters only from specified alphabet. So in can be converted to corresponding type
     * of sequence.
     *
     * @param alphabet alphabet
     * @param string   string to check
     * @return {@literal true} if sequence belongs to alphabet, {@literal false} if does not
     */
    public static boolean belongsToAlphabet(Alphabet<?> alphabet, String string) {
        for (int i = 0; i < string.length(); ++i)
            if (alphabet.codeFromSymbol(string.charAt(i)) == -1)
                return false;
        return true;
    }

    /**
     * Returns a set of possible alphabets for a given string.
     *
     * <p>Looks for alphabets registered in {@link com.milaboratory.core.sequence.Alphabets}.</p>
     *
     * @param string target string (sequence)
     * @return set of possible alphabets for a given string
     */
    public static Set<Alphabet<?>> possibleAlphabets(String string) {
        HashSet<Alphabet<?>> alphabets = new HashSet<>();
        for (Alphabet alphabet : Alphabets.getAll()) {
            if (belongsToAlphabet(alphabet, string))
                alphabets.add(alphabet);
        }
        return alphabets;
    }

    /**
     * Calculates number of mismatches (comparing position by position) between two regions of one or two different
     * sequences.
     *
     * @param seq0       first sequence
     * @param seq0Offset first letter of second region in first sequence
     * @param seq1       second sequence (may be the same as {@code seq0}
     * @param seq1Offset first letter of second region in second sequence
     * @param length     length of both regions
     * @param <S>        type of sequence
     * @return number of mismatches
     * @throws java.lang.IllegalArgumentException if one of regions is outside of target sequence
     */
    public static <S extends Sequence<S>> int mismatchCount(S seq0, int seq0Offset, S seq1, int seq1Offset, int length) {
        if (seq0.size() < seq0Offset + length || seq1.size() < seq1Offset + length)
            throw new IllegalArgumentException();

        int mm = 0;
        for (int i = 0; i < length; ++i)
            if (seq0.codeAt(i + seq0Offset) != seq1.codeAt(i + seq1Offset))
                ++mm;
        return mm;
    }

    /**
     * Returns a concatenation of several sequences.
     *
     * @param sequences array of sequences
     * @param <S>       type of sequences
     * @return concatenation of several sequences
     */
    public static <S extends Seq<S>> S concatenate(S... sequences) {
        if (sequences.length == 0)
            throw new IllegalArgumentException("Zero arguments");

        if (sequences.length == 1)
            return sequences[0];

        int size = 0;
        for (S s : sequences)
            size += s.size();

        SeqBuilder<S> builder = sequences[0].getBuilder().ensureCapacity(size);

        for (S s : sequences)
            builder.append(s);

        return builder.createAndDestroy();
    }
}
