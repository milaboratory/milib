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
public class IncompleteSequenceTest {

    @Test
    public void test1() throws Exception {
        Assert.assertEquals("AACCTT...AAA", new IncompleteNucleotideSequence("AACCTTxxxAAA").toString());
        Assert.assertEquals("AACCTT...AAA..CC",
                new IncompleteNucleotideSequence("AACCTTxxxAAA")
                        .concatenate(new IncompleteNucleotideSequence("yyCC")).toString());
    }

    @Test
    public void test2() throws Exception {
        Assert.assertEquals(new IncompleteNucleotideSequence("AACCTT...AAA..CC"),
                new IncompleteNucleotideSequence("AACCTTxxxAAA")
                        .concatenate(new IncompleteNucleotideSequence("yyCC")));
    }


    @Test(expected = RuntimeException.class)
    public void test3() throws Exception {
        IncompleteSequence seq = new IncompleteNucleotideSequence("AACCTT...AAA..CC");
        Assert.assertFalse(seq.isComplete());
        seq.convertToComplete();
    }

    @Test
    public void test4() throws Exception {
        IncompleteSequence seq = new IncompleteNucleotideSequence("AACCTTAAACC");
        Assert.assertTrue(seq.isComplete());
        Assert.assertEquals(new NucleotideSequence("AAccTTaAaCC"), seq.convertToComplete());
    }
}