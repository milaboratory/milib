/*
 * Copyright 2020 MiLaboratory, LLC
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
package com.milaboratory.util;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import cc.redberry.pipe.util.CountLimitingOutputPort;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.test.TestUtil;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class HashCollateTest {
    @Test
    public void test1() {
        List<NucleotideSequence> seqsList = new ArrayList<>();
        for (int i = 0; i < 1 << 15; i++)
            seqsList.add(TestUtil.randomSequence(NucleotideSequence.ALPHABET, 20, 200));

        RandomGenerator rg = new Well19937c(1234);

        OutputPort<NucleotideSequence> seqs = new OutputPort<NucleotideSequence>() {
            @Override
            public synchronized NucleotideSequence take() {
                return seqsList.get(rg.nextInt(seqsList.size()));
            }
        };
        seqs = new CountLimitingOutputPort<>(seqs, 10000000);

        File dir = TempFileManager.getTempDir();
        System.out.println(dir);

        HashCollate<NucleotideSequence> c = new HashCollate<>(
                NucleotideSequence.class,
                Objects::hashCode, Comparator.naturalOrder(),
                dir.toPath(), 5, 4, 6,
                PrimitivOState.INITIAL, PrimitivIState.INITIAL, 1 << 25);

        Comparator<NucleotideSequence> ec = c.effectiveComparator();

        OutputPortCloseable<NucleotideSequence> port = c.port(seqs);
        long p = 0;
        NucleotideSequence previous = null;
        for (NucleotideSequence ns : CUtils.it(port)) {
            ++p;
            if (previous != null) {
                int compare = ec.compare(previous, ns);
                Assert.assertTrue(compare <= 0);
            }
            previous = ns;
        }

        System.out.println(p);

        c.printStat();
    }
}