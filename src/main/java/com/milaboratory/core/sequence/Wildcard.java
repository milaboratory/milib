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

import com.milaboratory.util.HashFunctions;

import java.util.Arrays;

import static java.util.Arrays.binarySearch;

/**
 * Representation of a wildcard symbol.
 */
public final class Wildcard {
    /**
     * Symbol of wildcard
     */
    final char cSymbol;
    /**
     * Symbol of wildcard (byte)
     */
    final byte bSymbol;
    /**
     * Set of codes in wildcard
     */
    final byte[] matchingCodes;
    /**
     * Code representing this wildcard (e.g. code == codes[0] for pure letters)
     */
    final byte code;

    /**
     * Pure letter constructor
     *
     * @param cSymbol uppercase symbol
     * @param code    code
     */
    Wildcard(char cSymbol, byte code) {
        this(cSymbol, code, new byte[]{code});
    }

    /**
     * Wildcard constructor
     *
     * @param cSymbol       uppercase symbol of wildcard
     * @param code          code of wildcard
     * @param matchingCodes set of codes that this wildcards matches
     */
    Wildcard(char cSymbol, byte code, byte[] matchingCodes) {
        if (matchingCodes.length == 0 || Character.isLowerCase(cSymbol))
            throw new IllegalArgumentException();

        this.cSymbol = Character.toUpperCase(cSymbol);
        this.bSymbol = (byte) cSymbol;
        this.code = code;
        this.matchingCodes = matchingCodes.clone();

        // Sorting for binary search
        Arrays.sort(this.matchingCodes);

        // Assert for pure letters
        if (matchingCodes.length == 1 && code != matchingCodes[0])
            throw new IllegalArgumentException();

    }

    /**
     * Returns a wildcard letter.
     *
     * @return wildcard letter
     */
    public char getSymbol() {
        return cSymbol;
    }

    /**
     * Returns a number of letters corresponding to this wildcard. For example, for nucleotide wildcard 'R' the
     * corresponding nucleotides are 'A' and 'G', so size is 2.
     *
     * @return number of letters corresponding to this wildcard
     */
    public int count() {
        return matchingCodes.length;
    }

    /**
     * Returns {@literal true} if and only if this wildcards has only one matching letter, so it represents definite
     * letter and formally it is not a wildcard.
     *
     * @return {@literal true} if and only if this wildcards has only one matching letter, so it represents definite
     * letter and formally it is not a wildcard
     */
    public boolean isBasic() {
        return matchingCodes.length == 1;
    }

    /**
     * Returns <i>i-th</i> element of this wildcard.
     *
     * @param i index of letter
     * @return <i>i-th</i> element of this wildcard
     */
    public byte getMatchingCode(int i) {
        return matchingCodes[i];
    }

    /**
     * Returns alphabet code.
     *
     * @return alphabet code
     */
    public byte getCode() {
        return code;
    }

    /**
     * Returns whether this wildcard contains specified element (nucleotide or amino acid etc.).
     *
     * @param code binary code of element
     * @return true if this wildcard contains specified element and false otherwise
     */
    public boolean contains(byte code) {
        return binarySearch(matchingCodes, code) >= 0;
    }

    /**
     * Returns {@literal true} if set of symbols represented by this wildcard intersects with set of symbols
     * represented by {@code otherWildcard}.
     *
     * @param otherWildcard other wildcard
     * @return {@literal true} if set of symbols represented by this wildcard intersects with set of symbols represented
     * by {@code otherWildcard}
     */
    public boolean intersectsWith(Wildcard otherWildcard) {
        byte[] a = this.matchingCodes, b = otherWildcard.matchingCodes;
        int bPointer = 0, aPointer = 0;
        while (aPointer < a.length && bPointer < b.length)
            if (a[aPointer] == b[bPointer]) {
                return true;
            } else if (a[aPointer] < b[bPointer])
                aPointer++;
            else if (a[aPointer] > b[bPointer]) {
                bPointer++;
            }
        return false;
    }

    /**
     * Returns uniformly distributed element (nucleotide or amino acid et cetera) corresponding to this wildcard.
     * Note: for same seeds the result will be the same.
     *
     * @param seed seed
     * @return uniformly distributed symbol corresponding to this wildcard
     */
    public byte getUniformlyDistributedBasicCode(long seed) {
        if (isBasic())
            return code;

        seed = HashFunctions.JenkinWang64shift(seed);
        if (seed < 0) seed = -seed;
        return matchingCodes[(int) (seed % matchingCodes.length)];
    }
}
