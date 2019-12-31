/*
 * Copyright 2019 MiLaboratory, LLC
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
package com.milaboratory.core.tree;

import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.test.TestUtil;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class PrimerGenerator {
    @Test
    @Ignore
    public void generate() {
        Well19937c rg = new Well19937c();
        // Set<NucleotideSequence> bestGen4 = null;
        // for (int i = 0; i < 10; i++) {
        //     Set<NucleotideSequence> gen4 = generateSequences(rg, new HashSet<>(),
        //             TreeSearchParameters.FOUR_MISMATCHES_OR_INDELS, 6, 1_000_00, false);
        //     if (bestGen4 == null || gen4.size() > bestGen4.size()) {
        //         bestGen4 = gen4;
        //         System.out.println("New size (4) = " + bestGen4.size());
        //     }
        // }

        Set<NucleotideSequence> bestGen3 = null;
        for (int i = 0; i < 100; i++) {
            Set<NucleotideSequence> gen3 = generateSequences(rg, new HashSet<>(),
                    TreeSearchParameters.THREE_MISMATCHES_OR_INDELS, 6, 1_000_00, false);
            if (bestGen3 == null || gen3.size() > bestGen3.size()) {
                bestGen3 = gen3;
                System.out.println("New size (3) = " + bestGen3.size());
            }
        }

        // System.out.println("4");
        //
        // for (NucleotideSequence seq : bestGen4)
        //     System.out.println(seq);

        System.out.println("3");

        for (NucleotideSequence seq : bestGen3)
            System.out.println(seq);
    }

    public Set<NucleotideSequence> generateSequences(RandomGenerator rg,
                                                     Set<NucleotideSequence> exclude,
                                                     TreeSearchParameters treeSearchParameters,
                                                     int length,
                                                     long attempts,
                                                     boolean log) {
        SequenceTreeMap<NucleotideSequence, Boolean> tree = new SequenceTreeMap<>(NucleotideSequence.ALPHABET);
        for (NucleotideSequence seq : exclude)
            tree.put(seq, true);

        Set<NucleotideSequence> generated = new HashSet<>();
        for (int i = 0; i < attempts; i++) {
            NucleotideSequence newSeq = TestUtil.randomSequence(NucleotideSequence.ALPHABET, rg, length, length);
            int n = -1, l = 0, lm = 0;
            for (int j = 0; j < length; j++) {
                if(n != newSeq.codeAt(j)){
                    n = newSeq.codeAt(j);
                    l = 0;
                }
                ++l;
                lm = Math.max(l, lm);
            }
            if(lm > 4)
                continue;
            Boolean searchResult = tree.getNeighborhoodIterator(newSeq, treeSearchParameters).next();
            if (searchResult == null) {
                generated.add(newSeq);
                tree.put(newSeq, false);
                if (log)
                    System.out.println("Attempt = " + i + " generated = " + generated.size());
            }
        }

        return generated;
    }
}
