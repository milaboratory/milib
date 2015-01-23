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
public final class WildcardSymbol {
    final char cSymbol;
    final byte bSymbol;
    final byte[] codes;

    WildcardSymbol(char cSymbol, byte... codes) {
        this.cSymbol = cSymbol;
        this.bSymbol = (byte) cSymbol;
        this.codes = codes.clone();
        Arrays.sort(this.codes);
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
     * Returns a wildcard binary code.
     *
     * @return wildcard binary code
     */
    public byte getByteSymbol() {
        return bSymbol;
    }

    /**
     * Returns a number of letters corresponding to this wildcard. For example, for nucleotide wildcard 'R' the
     * corresponding nucleotides are 'A' and 'G', so size is 2.
     *
     * @return number of letters corresponding to this wildcard
     */
    public int size() {
        return codes.length;
    }

    /**
     * Returns <i>i-th</i> element of this wildcard.
     *
     * @param i number of letter
     * @return <i>i-th</i> element of this wildcard
     */
    public byte getCode(int i) {
        return codes[i];
    }

    /**
     * Returns whether this wildcard contains specified binary element (nucleotide or amino acid etc.).
     *
     * @param code binary code of element
     * @return true if this wildcard contains specified binary element and false otherwise
     */
    public boolean contains(byte code) {
        return binarySearch(codes, code) >= 0;
    }

    /**
     * Returns uniformly distributed element (nucleotide or amino acid et cetera) corresponding to this wildcard.
     * Note: for same seeds the result will be the same.
     *
     * @param seed seed
     * @return uniformly distributed symbol corresponding to this wildcard
     */
    public byte getUniformlyDistributedSymbol(long seed) {
        seed = HashFunctions.JenkinWang64shift(seed);
        if (seed < 0) seed = -seed;
        return codes[(int) (seed % codes.length)];
    }
}
