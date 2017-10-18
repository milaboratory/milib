package com.milaboratory.core.sequence;

import org.junit.Test;

import static org.junit.Assert.*;

public class NucleotideAlphabetCaseSensitiveTest {
    @Test
    public void isBasicTest() throws Exception {
        assertTrue(NucleotideAlphabetCaseSensitive.t_WILDCARD.isBasic());
        assertTrue(NucleotideAlphabetCaseSensitive.A_WILDCARD.isBasic());
        assertFalse(NucleotideAlphabetCaseSensitive.N_WILDCARD.isBasic());
        assertFalse(NucleotideAlphabetCaseSensitive.b_WILDCARD.isBasic());
    }

    @Test
    public void testMatches() throws Exception {
        WildcardTest.testMatches(NucleotideAlphabetCaseSensitive.INSTANCE);
    }
}
