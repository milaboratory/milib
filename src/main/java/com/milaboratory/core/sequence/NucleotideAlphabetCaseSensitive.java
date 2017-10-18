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
 * An alphabet for nucleotide sequences with lowercase and uppercase letters. This alphabet is the same
 * as NucleotideAlphabet, but lowercase letters mean that gaps and insertions are allowed between letters,
 * and for uppercase letters they are not allowed.
 *
 * @author Bolotin Dmitriy (bolotin.dmitriy@gmail.com)
 * @author Shugay Mikhail (mikhail.shugay@gmail.com)
 * @author Popov Aleksandr (alexander230r@gmail.com)
 * @see com.milaboratory.core.sequence.NucleotideAlphabet
 * @see com.milaboratory.core.sequence.NucleotideSequenceCaseSensitive
 */
public final class NucleotideAlphabetCaseSensitive extends AbstractArrayAlphabet<NucleotideSequenceCaseSensitive> {
    /**
     * Adenine byte representation, uppercase
     */
    public static final byte A = 0;
    /**
     * Guanine byte representation, uppercase
     */
    public static final byte G = 1;
    /**
     * Cytosine byte representation, uppercase
     */
    public static final byte C = 2;
    /**
     * Thymine byte representation, uppercase
     */
    public static final byte T = 3;
    /**
     * Adenine byte representation, lowercase
     */
    public static final byte a = 4;
    /**
     * Guanine byte representation, lowercase
     */
    public static final byte g = 5;
    /**
     * Cytosine byte representation, lowercase
     */
    public static final byte c = 6;
    /**
     * Thymine byte representation, lowercase
     */
    public static final byte t = 7;

    /* Codes for wildcards */
    /**
     * any Nucleotide, uppercase
     */
    public static final byte N = 8;
    /**
     * any Nucleotide, lowercase
     */
    public static final byte n = 9;

    /* Two-letter wildcard */
    /**
     * puRine, uppercase
     */
    public static final byte R = 10;
    /**
     * pYrimidine, uppercase
     */
    public static final byte Y = 11;
    /**
     * Strong, uppercase
     */
    public static final byte S = 12;
    /**
     * Weak, uppercase
     */
    public static final byte W = 13;
    /**
     * Keto, uppercase
     */
    public static final byte K = 14;
    /**
     * aMino, uppercase
     */
    public static final byte M = 15;
    /**
     * puRine, lowercase
     */
    public static final byte r = 16;
    /**
     * pYrimidine, lowercase
     */
    public static final byte y = 17;
    /**
     * Strong, lowercase
     */
    public static final byte s = 18;
    /**
     * Weak, lowercase
     */
    public static final byte w = 19;
    /**
     * Keto, lowercase
     */
    public static final byte k = 20;
    /**
     * aMino, lowercase
     */
    public static final byte m = 21;

    /* Three-letter wildcard */
    /**
     * not A (B comes after A), uppercase
     */
    public static final byte B = 22;
    /**
     * not C (D comes after C), uppercase
     */
    public static final byte D = 23;
    /**
     * not G (H comes after G), uppercase
     */
    public static final byte H = 24;
    /**
     * not T (V comes after T and U), uppercase
     */
    public static final byte V = 25;
    /**
     * not A (B comes after A), lowercase
     */
    public static final byte b = 26;
    /**
     * not C (D comes after C), lowercase
     */
    public static final byte d = 27;
    /**
     * not G (H comes after G), lowercase
     */
    public static final byte h = 28;
    /**
     * not T (V comes after T and U), lowercase
     */
    public static final byte v = 29;

    /* Wildcards */

    /* Basic wildcards */
    /**
     * Adenine byte representation
     */
    public static final Wildcard A_WILDCARD = new Wildcard('A', A, 1, new byte[]{A, N, R, W, M, D, H, V});
    public static final Wildcard a_WILDCARD = new Wildcard('a', a, 1, new byte[]{A, N, R, W, M, D, H, V});
    /**
     * Guanine byte representation
     */
    public static final Wildcard G_WILDCARD = new Wildcard('G', G, 1, new byte[]{G, N, R, S, K, B, D, V});
    public static final Wildcard g_WILDCARD = new Wildcard('g', g, 1, new byte[]{G, N, R, S, K, B, D, V});
    /**
     * Cytosine byte representation
     */
    public static final Wildcard C_WILDCARD = new Wildcard('C', C, 1, new byte[]{C, N, Y, S, M, B, H, V});
    public static final Wildcard c_WILDCARD = new Wildcard('c', c, 1, new byte[]{C, N, Y, S, M, B, H, V});
    /**
     * Thymine byte representation
     */
    public static final Wildcard T_WILDCARD = new Wildcard('T', T, 1, new byte[]{T, N, Y, W, K, B, D, H});
    public static final Wildcard t_WILDCARD = new Wildcard('t', t, 1, new byte[]{T, N, Y, W, K, B, D, H});

    /* N wildcard */
    /**
     * any Nucleotide
     */
    public static final Wildcard N_WILDCARD = new Wildcard('N', N, 4, new byte[]{A, G, C, T, N, R, Y, S, W, K, M, B, D, H, V});
    public static final Wildcard n_WILDCARD = new Wildcard('n', n, 4, new byte[]{A, G, C, T, N, R, Y, S, W, K, M, B, D, H, V});

    /* Two-letter wildcards */
    /**
     * puRine
     */
    public static final Wildcard R_WILDCARD = new Wildcard('R', R, 2, new byte[]{A, G, N, R, S, W, K, M, B, D, H, V});
    public static final Wildcard r_WILDCARD = new Wildcard('r', r, 2, new byte[]{A, G, N, R, S, W, K, M, B, D, H, V});
    /**
     * pYrimidine
     */
    public static final Wildcard Y_WILDCARD = new Wildcard('Y', Y, 2, new byte[]{C, T, N, Y, S, W, K, M, B, D, H, V});
    public static final Wildcard y_WILDCARD = new Wildcard('y', y, 2, new byte[]{C, T, N, Y, S, W, K, M, B, D, H, V});
    /**
     * Strong
     */
    public static final Wildcard S_WILDCARD = new Wildcard('S', S, 2, new byte[]{G, C, N, R, Y, S, K, M, B, D, H, V});
    public static final Wildcard s_WILDCARD = new Wildcard('s', s, 2, new byte[]{G, C, N, R, Y, S, K, M, B, D, H, V});
    /**
     * Weak
     */
    public static final Wildcard W_WILDCARD = new Wildcard('W', W, 2, new byte[]{A, T, N, R, Y, W, K, M, B, D, H, V});
    public static final Wildcard w_WILDCARD = new Wildcard('w', w, 2, new byte[]{A, T, N, R, Y, W, K, M, B, D, H, V});
    /**
     * Keto
     */
    public static final Wildcard K_WILDCARD = new Wildcard('K', K, 2, new byte[]{G, T, N, R, Y, S, W, K, B, D, H, V});
    public static final Wildcard k_WILDCARD = new Wildcard('k', k, 2, new byte[]{G, T, N, R, Y, S, W, K, B, D, H, V});
    /**
     * aMino
     */
    public static final Wildcard M_WILDCARD = new Wildcard('M', M, 2, new byte[]{A, C, N, R, Y, S, W, M, B, D, H, V});
    public static final Wildcard m_WILDCARD = new Wildcard('m', m, 2, new byte[]{A, C, N, R, Y, S, W, M, B, D, H, V});

    /* Three-letter wildcards */
    /**
     * not A (B comes after A)
     */
    public static final Wildcard B_WILDCARD = new Wildcard('B', B, 3, new byte[]{G, C, T, N, R, Y, S, W, K, M, B, D, H, V});
    public static final Wildcard b_WILDCARD = new Wildcard('b', b, 3, new byte[]{G, C, T, N, R, Y, S, W, K, M, B, D, H, V});
    /**
     * not C (D comes after C)
     */
    public static final Wildcard D_WILDCARD = new Wildcard('D', D, 3, new byte[]{A, G, T, N, R, Y, S, W, K, M, B, D, H, V});
    public static final Wildcard d_WILDCARD = new Wildcard('d', d, 3, new byte[]{A, G, T, N, R, Y, S, W, K, M, B, D, H, V});
    /**
     * not G (H comes after G)
     */
    public static final Wildcard H_WILDCARD = new Wildcard('H', H, 3, new byte[]{A, C, T, N, R, Y, S, W, K, M, B, D, H, V});
    public static final Wildcard h_WILDCARD = new Wildcard('h', h, 3, new byte[]{A, C, T, N, R, Y, S, W, K, M, B, D, H, V});
    /**
     * not T (V comes after T and U)
     */
    public static final Wildcard V_WILDCARD = new Wildcard('V', V, 3, new byte[]{A, G, C, N, R, Y, S, W, K, M, B, D, H, V});
    public static final Wildcard v_WILDCARD = new Wildcard('v', v, 3, new byte[]{A, G, C, N, R, Y, S, W, K, M, B, D, H, V});

    /**
     * All wildcards array. Each wildcard has index equals to its code.
     */
    private static final Wildcard[] WILDCARDS;

    /**
     * Singleton instance.
     */
    final static NucleotideAlphabetCaseSensitive INSTANCE = new NucleotideAlphabetCaseSensitive();

    private NucleotideAlphabetCaseSensitive() {
        super("nucleotide_case_sensitive", (byte) 3, 8, null,
                A_WILDCARD, T_WILDCARD, G_WILDCARD, C_WILDCARD,
                a_WILDCARD, t_WILDCARD, g_WILDCARD, c_WILDCARD,
                N_WILDCARD, n_WILDCARD,
                R_WILDCARD, Y_WILDCARD, S_WILDCARD, W_WILDCARD, K_WILDCARD, M_WILDCARD,
                r_WILDCARD, y_WILDCARD, s_WILDCARD, w_WILDCARD, k_WILDCARD, m_WILDCARD,
                B_WILDCARD, D_WILDCARD, H_WILDCARD, V_WILDCARD,
                b_WILDCARD, d_WILDCARD, h_WILDCARD, v_WILDCARD);
    }

    /**
     * Returns UTF-8 character corresponding to specified byte-code.
     *
     * @param code byte-code of nucleotide
     * @return UTF-8 character corresponding to specified byte-code
     */
    public static byte symbolByteFromCode(byte code) {
        return (byte) INSTANCE.codeToSymbol(code);
    }

    public static byte byteSymbolToCode(byte symbol) {
        return INSTANCE.symbolToCode((char) symbol);
    }

    @Override
    NucleotideSequenceCaseSensitive createUnsafe(byte[] array) {
        return new NucleotideSequenceCaseSensitive(array, true);
    }

    static {
        WILDCARDS = INSTANCE.getAllWildcards().toArray(new Wildcard[INSTANCE.size()]);
    }
}
