package com.milaboratory.core.sequence;

import org.junit.Test;

import static org.junit.Assert.*;

public class NucleotideSequenceCaseSensitiveTest {
    @Test
    public void test1() {
        NucleotideSequenceCaseSensitive sequence = new NucleotideSequenceCaseSensitive("ATTAGACatagaCA");
        assertEquals(sequence.toString(), "ATTAGACatagaCA");
        NucleotideSequenceCaseSensitive subSequence = sequence.getRange(0, sequence.size());
        assertEquals(subSequence.toString(), "ATTAGACatagaCA");
        assertEquals(subSequence.hashCode(), sequence.hashCode());
        assertEquals(subSequence, sequence);

        NucleotideSequenceCaseSensitive sequence1 = new NucleotideSequenceCaseSensitive("AGACatagaCA");
        NucleotideSequenceCaseSensitive subSequence1 = sequence.getRange(3, sequence.size());

        assertEquals(subSequence1.hashCode(), sequence1.hashCode());
        assertTrue(subSequence1.hashCode() != new NucleotideSequenceCaseSensitive("agacatagaca").hashCode());
        assertEquals(subSequence1, sequence1);
        assertEquals(NucleotideSequenceCaseSensitive.EMPTY, new NucleotideSequenceCaseSensitive(""));
    }

    @Test
    public void testConcatenate1() throws Exception {
        NucleotideSequenceCaseSensitive s1 = new NucleotideSequenceCaseSensitive("ATTAgaca"),
                s2 = new NucleotideSequenceCaseSensitive("GACatATA");

        assertEquals(new NucleotideSequenceCaseSensitive("ATTAgacaGACatATA"), s1.concatenate(s2));
    }

    @Test
    public void testConcatenate2() throws Exception {
        NucleotideSequenceCaseSensitive s1 = new NucleotideSequenceCaseSensitive("attagaca"),
                s2 = new NucleotideSequenceCaseSensitive("");

        assertEquals(new NucleotideSequenceCaseSensitive("attagaca"), s1.concatenate(s2));
        assertEquals(new NucleotideSequenceCaseSensitive("attagaca"), s2.concatenate(s1));
    }

    @Test
    public void testConcatenate3() throws Exception {
        NucleotideSequenceCaseSensitive s1 = new NucleotideSequenceCaseSensitive(""),
                s2 = new NucleotideSequenceCaseSensitive("");

        assertEquals(new NucleotideSequenceCaseSensitive(""), s1.concatenate(s2));
        assertEquals(new NucleotideSequenceCaseSensitive(""), s2.concatenate(s1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownSymbol1() throws Exception {
        new NucleotideSequenceCaseSensitive("ATTANQ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownSymbol2() throws Exception {
        new NucleotideSequenceCaseSensitive(new char[]{'a', 'n', 'q'});
    }

    @Test
    public void toNucleotideSequenceTest() throws Exception {
        assertEquals("ATTAGACA",
                new NucleotideSequenceCaseSensitive("AttAGacA").toNucleotideSequence().toString());
        assertEquals(new NucleotideSequence("ATGCNWBD").hashCode(),
                new NucleotideSequenceCaseSensitive("aTgcnWbd").toNucleotideSequence().hashCode());
    }
}
