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
 * An alphabet for nucleotide sequences. This alphabet defines the following mapping:
 *
 * <p>0 - 'A', 1 - 'G', 2 - 'C', 3 - 'T'</p>
 *
 * <p> This class also defines wildcards as specified by IUPAC: 'R' for 'A' or 'G', 'Y' for 'C' or 'T' etc. </p>
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @see com.milaboratory.core.sequence.Alphabet
 * @see com.milaboratory.core.sequence.NucleotideSequence
 */
public final class NucleotideAlphabet extends AbstractArrayAlphabet<NucleotideSequence> {
    /**
     * Adenine byte representation
     */
    public static final byte A = 0;
    /**
     * Guanine byte representation
     */
    public static final byte G = 1;
    /**
     * Cytosine byte representation
     */
    public static final byte C = 2;
    /**
     * Thymine byte representation
     */
    public static final byte T = 3;

    /* Codes for wildcards */
    /**
     * any Nucleotide
     */
    public static final byte N = 4;

    /* Two-letter wildcard */
    /**
     * puRine
     */
    public static final byte R = 5;
    /**
     * pYrimidine
     */
    public static final byte Y = 6;
    /**
     * Strong
     */
    public static final byte S = 7;
    /**
     * Weak
     */
    public static final byte W = 8;
    /**
     * Keto
     */
    public static final byte K = 9;
    /**
     * aMino
     */
    public static final byte M = 10;

    /* Three-letter wildcard */
    /**
     * not A (B comes after A)
     */
    public static final byte B = 11;
    /**
     * not C (D comes after C)
     */
    public static final byte D = 12;
    /**
     * not G (H comes after G)
     */
    public static final byte H = 13;
    /**
     * not T (V comes after T and U)
     */
    public static final byte V = 14;


    /* Wildcards */

    /* Basic wildcards */
    /**
     * Adenine byte representation
     */
    public static final Wildcard A_WILDCARD = new Wildcard('A', A);
    /**
     * Guanine byte representation
     */
    public static final Wildcard G_WILDCARD = new Wildcard('G', G);
    /**
     * Cytosine byte representation
     */
    public static final Wildcard C_WILDCARD = new Wildcard('C', C);
    /**
     * Thymine byte representation
     */
    public static final Wildcard T_WILDCARD = new Wildcard('T', T);

    /* N wildcard */
    /**
     * any Nucleotide
     */
    public static final Wildcard N_WILDCARD = new Wildcard('N', N, new byte[]{A, T, G, C});

    /* Two-letter wildcards */
    /**
     * puRine
     */
    public static final Wildcard R_WILDCARD = new Wildcard('R', R, new byte[]{A, G});
    /**
     * pYrimidine
     */
    public static final Wildcard Y_WILDCARD = new Wildcard('Y', Y, new byte[]{C, T});
    /**
     * Strong
     */
    public static final Wildcard S_WILDCARD = new Wildcard('S', S, new byte[]{G, C});
    /**
     * Weak
     */
    public static final Wildcard W_WILDCARD = new Wildcard('W', W, new byte[]{A, T});
    /**
     * Keto
     */
    public static final Wildcard K_WILDCARD = new Wildcard('K', K, new byte[]{G, T});
    /**
     * aMino
     */
    public static final Wildcard M_WILDCARD = new Wildcard('M', M, new byte[]{A, C});

    /* Three-letter wildcards */
    /**
     * not A (B comes after A)
     */
    public static final Wildcard B_WILDCARD = new Wildcard('B', B, new byte[]{C, G, T});
    /**
     * not C (D comes after C)
     */
    public static final Wildcard D_WILDCARD = new Wildcard('D', D, new byte[]{A, G, T});
    /**
     * not G (H comes after G)
     */
    public static final Wildcard H_WILDCARD = new Wildcard('H', H, new byte[]{A, C, T});
    /**
     * not T (V comes after T and U)
     */
    public static final Wildcard V_WILDCARD = new Wildcard('V', V, new byte[]{A, C, G});

    /**
     * Singleton instance.
     */
    final static NucleotideAlphabet INSTANCE = new NucleotideAlphabet();

    private NucleotideAlphabet() {
        super("nucleotide", (byte) 1, 4,
                // Any letter
                N_WILDCARD,
                // Content
                A_WILDCARD, T_WILDCARD, G_WILDCARD, C_WILDCARD,
                N_WILDCARD,
                R_WILDCARD, Y_WILDCARD, S_WILDCARD, W_WILDCARD, K_WILDCARD, M_WILDCARD,
                B_WILDCARD, D_WILDCARD, H_WILDCARD, V_WILDCARD);
    }

    /**
     * Returns a complement nucleotide.
     *
     * @param nucleotide byte value of nucleotide
     * @return complement nucleotide to the specified one
     */
    public static byte getComplement(byte nucleotide) {
        if (nucleotide < 4)
            return (byte) (nucleotide ^ 3);
        else
            throw new UnsupportedOperationException("Not implemeted yet.");
    }

    /**
     * Returns UTF-8 character corresponding to specified byte-code.
     *
     * @param code byte-code of nucleotide
     * @return UTF-8 character corresponding to specified byte-code
     */
    public static byte symbolByteFromCode(byte code) {
        //TODO optimize
        return (byte) INSTANCE.codeToSymbol(code);
    }

    public static byte byteSymbolToCode(byte symbol) {
        //TODO optimize
        return INSTANCE.symbolToCode((char) symbol);
    }

    @Override
    NucleotideSequence createUnsafe(byte[] array) {
        return new NucleotideSequence(array, true);
    }
}
