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

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class AminoAcidSequenceTest {
    @Test
    public void test1() throws Exception {
        Assert.assertEquals(new AminoAcidSequence("IR_"),
                AminoAcidSequence.translate(true, true, new NucleotideSequence("ATTAGACA")));
        Assert.assertEquals(new AminoAcidSequence("_*T"),
                AminoAcidSequence.translate(false, true, new NucleotideSequence("ATTAGACA")));

        Assert.assertEquals(new AminoAcidSequence("I_T"),
                AminoAcidSequence.translate(null, true, new NucleotideSequence("ATTAGACA")));

        Assert.assertEquals(new AminoAcidSequence("IR"),
                AminoAcidSequence.translate(true, true, new NucleotideSequence("ATTAGA")));
        Assert.assertEquals(new AminoAcidSequence("IR"),
                AminoAcidSequence.translate(false, true, new NucleotideSequence("ATTAGA")));

        Assert.assertEquals(new AminoAcidSequence("IR"),
                AminoAcidSequence.translate(null, true, new NucleotideSequence("ATTAGA")));
    }

    @Test
    public void test2() throws Exception {
        Assert.assertEquals(new AminoAcidSequence("IR"),
                AminoAcidSequence.translate(true, false, new NucleotideSequence("ATTAGACA")));
        Assert.assertEquals(new AminoAcidSequence("*T"),
                AminoAcidSequence.translate(false, false, new NucleotideSequence("ATTAGACA")));

        Assert.assertEquals(new AminoAcidSequence("IR"),
                AminoAcidSequence.translate(true, false, new NucleotideSequence("ATTAGA")));
        Assert.assertEquals(new AminoAcidSequence("IR"),
                AminoAcidSequence.translate(false, false, new NucleotideSequence("ATTAGA")));
    }

    @Test
    public void test3() throws Exception {
        Assert.assertEquals(new AminoAcidSequence("_"),
                AminoAcidSequence.translate(false, true, new NucleotideSequence("AT")));
        Assert.assertEquals(new AminoAcidSequence("_"),
                AminoAcidSequence.translate(true, true, new NucleotideSequence("AT")));
        Assert.assertEquals(new AminoAcidSequence("_"),
                AminoAcidSequence.translate(null, true, new NucleotideSequence("AT")));

        Assert.assertEquals(new AminoAcidSequence(""),
                AminoAcidSequence.translate(false, true, new NucleotideSequence("")));
        Assert.assertEquals(new AminoAcidSequence(""),
                AminoAcidSequence.translate(true, true, new NucleotideSequence("")));
        Assert.assertEquals(new AminoAcidSequence(""),
                AminoAcidSequence.translate(null, true, new NucleotideSequence("")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownSymbol1() throws Exception {
        new AminoAcidSequence("ATTAGACANX");
    }
}