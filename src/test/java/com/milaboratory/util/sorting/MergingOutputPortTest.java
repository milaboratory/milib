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
package com.milaboratory.util.sorting;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import com.milaboratory.core.Range;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.SequenceProperties;
import com.milaboratory.core.sequence.SequencesUtils;
import com.milaboratory.test.TestUtil;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class MergingOutputPortTest {
    @Test
    public void test1() throws InterruptedException {
        ArrayList<ArrayList<Integer>> ints = new ArrayList<>();
        RandomGenerator rg = new Well19937c();
        for (int i = 0; i < 30; i++) {
            ArrayList<Integer> array = new ArrayList<>();
            int n = rg.nextInt(50);
            for (int j = 0; j < n; j++) {
                int k = rg.nextInt(50);
                array.add(k << 8 | i);
            }
            array.sort(Comparator.naturalOrder());
            ints.add(array);
        }

        MergingOutputPort<Integer> mop = new MergingOutputPort<>(
                Comparator.comparing(i -> i >>> 8),
                ints.stream()
                        .map(CUtils::asOutputPort)
                        .collect(Collectors.toList()));

        ArrayList<Integer> result = new ArrayList<>();
        CUtils.drain(mop, result::add);
        result.remove(result.size() - 1);

        List<Integer> expected = ints.stream()
                .flatMap(Collection::stream)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());

        Assert.assertEquals(expected, result);
    }

    @Test
    public void joinTest1() throws InterruptedException {
        int N = 300;
        int K = 5000;

        Set<NucleotideSequence> markerSequencesSet = new HashSet<>();
        while (markerSequencesSet.size() < N)
            markerSequencesSet.add(TestUtil.randomSequence(NucleotideSequence.ALPHABET, 10, 10));
        NucleotideSequence[] mSequences = markerSequencesSet.toArray(new NucleotideSequence[0]);

        List<SequenceProperties.Subsequence<NucleotideSequence>> stream = Arrays.asList(
                new SequenceProperties.Subsequence<>(new Range(10, 12)),
                new SequenceProperties.Subsequence<>(new Range(17, 20))
        );

        List<SequenceProperties.Subsequence<NucleotideSequence>> target = Arrays.asList(
                new SequenceProperties.Subsequence<>(new Range(10, 13)),
                new SequenceProperties.Subsequence<>(new Range(17, 18))
        );

        List<List<NucleotideSequence>> seqs = new ArrayList<>();
        RandomGenerator rg = new Well19937c();
        for (int i = 0; i < N; i++) {
            ArrayList<NucleotideSequence> array = new ArrayList<>();
            int n = rg.nextInt(K);
            for (int j = 0; j < n; j++) {
                array.add(mSequences[i].concatenate(TestUtil.randomSequence(NucleotideSequence.ALPHABET, 10, 10)));
            }
            array.sort(SortingUtil.combine(stream));
            seqs.add(new ArrayList<>(array));
        }

        MergeStrategy<NucleotideSequence> mergeStrategy = MergeStrategy.calculateStrategy(stream, target);
        List<OutputPort<NucleotideSequence>> ops = seqs.stream()
                .map(CUtils::asOutputPort)
                .collect(Collectors.toList());

        List<List<List<NucleotideSequence>>> result = new ArrayList<>();
        OutputPortCloseable<List<List<NucleotideSequence>>> join = MergingOutputPort.join(mergeStrategy, ops);
        CUtils.drain(join, result::add);
        result.remove(result.size() - 1);

        Map<NucleotideSequence, List<NucleotideSequence>[]> expectedMap = new HashMap<>();
        for (int i = 0; i < seqs.size(); i++) {
            List<NucleotideSequence> sseqs = seqs.get(i);
            for (NucleotideSequence seq : sseqs) {
                NucleotideSequence key = SequencesUtils.concatenate(target.stream().map(t -> t.get(seq)).toArray(NucleotideSequence[]::new));
                List<NucleotideSequence>[] r = expectedMap.computeIfAbsent(key, __ -> {
                    List[] lsts = new List[seqs.size()];
                    for (int j = 0; j < lsts.length; j++)
                        lsts[j] = new ArrayList();
                    return lsts;
                });
                List<NucleotideSequence> b = r[i];
                b.add(seq);
            }
        }
        
        Assert.assertEquals(
                expectedMap.values().stream().map(Arrays::asList).collect(Collectors.toSet()),
                new HashSet<>(result)
        );
    }
}