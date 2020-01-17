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
import com.milaboratory.core.sequence.SequencesUtils;
import com.milaboratory.test.TestUtil;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

public class PrimerGenerator {

    @Test
    @Ignore
    public void generate() throws FileNotFoundException {
        Well19937c rg = new Well19937c();

        for (int length = 6; length <= 7; length++) {
            for (int minDistance = 2; minDistance <= 4; minDistance++) {
                for (int maxHomopolymer = 2; maxHomopolymer <= 5; maxHomopolymer++) {
                    Set<NucleotideSequence> best = null;
                    for (int i = 0; i < 100; i++) {
                        TreeSearchParameters searchParameters;
                        switch (minDistance) {
                            case 2:
                                searchParameters = TreeSearchParameters.TWO_MISMATCHES_OR_INDELS;
                                break;
                            case 3:
                                searchParameters = TreeSearchParameters.THREE_MISMATCHES_OR_INDELS;
                                break;
                            case 4:
                                searchParameters = TreeSearchParameters.FOUR_MISMATCHES_OR_INDELS;
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                        Set<NucleotideSequence> gen = generateSequences(rg,
                                new HashSet<>(),
                                searchParameters,
                                length, maxHomopolymer, 1_000_00,
                                false);
                        if (best == null || gen.size() > best.size())
                            best = gen;
                    }
                    System.out.println("Best ford length=" + length +
                            " minDistance=" + minDistance +
                            " maxHomo=" + maxHomopolymer +
                            ": " + best.size());
                    try (PrintStream ps = new PrintStream(
                            "length" + length +
                                    "_minDistance" + minDistance +
                                    "_maxHomo" + maxHomopolymer + ".txt")) {
                        for (NucleotideSequence seq : best)
                            ps.println(seq);
                    }
                }
            }
        }


        // System.out.println("4");
        //
        // for (NucleotideSequence seq : bestGen4)
        //     System.out.println(seq);

        // System.out.println("3");
        //
        // for (NucleotideSequence seq : best)
        //     System.out.println(seq);
    }

    public Set<NucleotideSequence> generateSequences(RandomGenerator rg,
                                                     Set<NucleotideSequence> exclude,
                                                     TreeSearchParameters treeSearchParameters,
                                                     int length,
                                                     int maxHomopolymerLength,
                                                     long attempts,
                                                     boolean log) {
        SequenceTreeMap<NucleotideSequence, Boolean> tree = new SequenceTreeMap<>(NucleotideSequence.ALPHABET);
        for (NucleotideSequence seq : exclude)
            tree.put(seq, true);

        Set<NucleotideSequence> generated = new HashSet<>();
        for (int i = 0; i < attempts; i++) {
            NucleotideSequence newSeq = TestUtil.randomSequence(NucleotideSequence.ALPHABET, rg, length, length);
            if (SequencesUtils.findLongestHomopolymer(newSeq).length() > maxHomopolymerLength)
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
