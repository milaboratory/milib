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
package com.milaboratory.core.motif;

import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.core.sequence.WildcardSymbol;
import com.milaboratory.core.sequence.WithWildcards;
import com.milaboratory.util.BitArray;

import java.util.Arrays;

public final class Motif<S extends Sequence<S>> {
    private final Alphabet<S> alphabet;
    private final int size;
    final BitArray data;

    Motif(Alphabet<S> alphabet, int size, BitArray data) {
        if (!dataConsistent(data, size))
            throw new IllegalArgumentException("Inconsistent data. Some positions in motif has no possible values.");
        this.alphabet = alphabet;
        this.size = size;
        this.data = data;
    }

    public Motif(S sequence) {
        this.alphabet = sequence.getAlphabet();
        this.size = sequence.size();
        int alphabetSize = alphabet.size();
        this.data = new BitArray(alphabetSize * size);
        for (int i = 0; i < size; ++i)
            data.set(sequence.codeAt(i) * size + i);
    }

    public Motif(Alphabet<S> alphabet, String motif) {
        this.alphabet = alphabet;
        this.size = motif.length();
        int alphabetSize = alphabet.size();
        this.data = new BitArray(alphabetSize * size);
        if (alphabet instanceof WithWildcards) {
            WithWildcards wildcardAlphabet = (WithWildcards) alphabet;
            for (int i = 0; i < size; ++i) {
                final WildcardSymbol wildcard = wildcardAlphabet.getWildcardFor(motif.charAt(i));
                if (wildcard == null)
                    throw new IllegalArgumentException("Unknown wildcard " + motif.charAt(i));
                for (int j = 0; j < wildcard.size(); ++j)
                    data.set(wildcard.getCode(j) * size + i);
            }
        } else {
            for (int i = 0; i < size; ++i) {
                byte code = alphabet.codeFromSymbol(motif.charAt(i));
                if (code == -1)
                    throw new IllegalArgumentException("Unknown symbol " + motif.charAt(i));
                data.set(code * size + i);
            }
        }
    }

    public BitapPattern toBitapPattern() {
        if (size >= 64)
            throw new RuntimeException("Supports motifs with length less then 64.");
        int aSize = alphabet.size();
        long[] patternMask = new long[aSize],
                reversePatternMask = new long[aSize];
        Arrays.fill(patternMask, ~0);
        Arrays.fill(reversePatternMask, ~0);
        int p = 0;
        for (int i = 0; i < aSize; ++i)
            for (int j = 0; j < size; ++j)
                if (data.get(p++)) {
                    patternMask[i] &= ~(1L << j);
                    reversePatternMask[i] &= ~(1L << (size - j - 1));
                }
        return new BitapPattern(size, patternMask, reversePatternMask);
    }

    public int size() {
        return size;
    }

    public boolean allows(byte code, int position) {
        return data.get(code * size + position);
    }

    public boolean matches(S sequence, int from) {
        if (from < 0 || from + size > sequence.size())
            throw new IndexOutOfBoundsException();
        for (int i = 0; i < size; ++i)
            if (!allows(sequence.codeAt(from++), i))
                return false;
        return true;
    }

    private final static boolean dataConsistent(BitArray data, int size) {
        int i = 0;
        while (i * size < data.size()) {
            if (data.get(i))
                i = ((i / size) + 1) * size;
            ++i;
            if (i % size == 0)
                return false;
        }
        return true;
    }
}
