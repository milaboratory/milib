/*
 * Copyright 2016 MiLaboratory.com
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
package com.milaboratory.util.graph;

import com.milaboratory.util.BitArrayInt;
import com.milaboratory.util.RandomUtil;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.milaboratory.util.BitArrayInt.createSet;

/**
 * Created by dbolotin on 08/02/16.
 */
public class AdjacencyMatrixTest {
    @Test
    public void test1() throws Exception {
        AdjacencyMatrix matrix = new AdjacencyMatrix(6);
        matrix.setConnected(0, 4);
        matrix.setConnected(0, 1);
        matrix.setConnected(1, 4);
        matrix.setConnected(1, 2);
        matrix.setConnected(2, 3);
        matrix.setConnected(3, 4);
        matrix.setConnected(3, 5);
        List<BitArrayInt> cliques = matrix.calculateMaximalCliques();
        HashSet<BitArrayInt> set = new HashSet<>(cliques);

        Assert.assertEquals(cliques.size(), set.size());
        Assert.assertEquals(5, set.size());

        Assert.assertTrue(set.contains(createSet(6, 0, 1, 4)));
        Assert.assertTrue(set.contains(createSet(6, 1, 2)));
        Assert.assertTrue(set.contains(createSet(6, 2, 3)));
        Assert.assertTrue(set.contains(createSet(6, 3, 4)));
        Assert.assertTrue(set.contains(createSet(6, 3, 5)));
    }

    @Test
    public void test2() throws Exception {
        TestData testData = generateTestData(130, 5, 5, 10, 10);
        AdjacencyMatrix matrix = testData.matrix;
        for (BitArrayInt bitArrayInt : matrix.calculateMaximalCliques()) {
            if (bitArrayInt.bitCount() >= 5)
                System.out.println(bitArrayInt);
        }
//        System.out.println(matrix);

    }

    @Test
    public void test3() throws Exception {
        RandomUtil.reseedThreadLocalFromTime();
        for (int i = 0; i < 100; i++)
            assertTestData(generateTestData(150, 10, 10, 15, 30), 10);
    }

    static void assertTestData(TestData testData, int minCliqueSize) {
        HashSet<BitArrayInt> actual = new HashSet<>();
        List<BitArrayInt> cliques = testData.matrix.calculateMaximalCliques();
        for (BitArrayInt clique : cliques)
            if (clique.bitCount() >= minCliqueSize)
                actual.add(clique);

        HashSet<BitArrayInt> expected = new HashSet<>(Arrays.asList(testData.cliques));
        Assert.assertEquals(expected, actual);
    }

    static TestData generateTestData(int size, int nCliques,
                                     int minCliqueSize, int maxCliqueSize,
                                     int noisePoints) {
        Well19937c random = RandomUtil.getThreadLocalRandom();
        BitArrayInt[] cliques = new BitArrayInt[nCliques];

        AdjacencyMatrix matrix = new AdjacencyMatrix(size);
        int generated = 0;
        final int[] mask = new int[size];
        for (int i = 0; i < nCliques; i++) {
            int cSize = minCliqueSize + random.nextInt(maxCliqueSize - minCliqueSize);
            TIntSet tClique = new TIntHashSet();
            int maskSet = 0;
            while (tClique.size() < cSize) {
//                if (generated == size)
//                    break;
                int v = random.nextInt(size);
                if (mask[v] != 0) {
                    ++maskSet;
                    if (maskSet >= 3)
                        continue;
                }

                mask[v] = 1;
                ++generated;
                tClique.add(v);
            }

            cliques[i] = new BitArrayInt(size);
            int[] vs = tClique.toArray();
            for (int j = 0; j < vs.length; j++)
                cliques[i].set(vs[j]);

            for (int j = 0; j < vs.length; j++)
                for (int k = j + 1; k < vs.length; k++)
                    matrix.setConnected(vs[j], vs[k]);
        }

        //introduce noise

        for (int i = 0; i < noisePoints; i++) {
            int a = random.nextInt(size), b = random.nextInt(size);
            if (a == b) {
                --i;
                continue;
            }
            matrix.setConnected(a, b);
        }

        return new TestData(matrix, cliques);
    }

    static class TestData {
        final AdjacencyMatrix matrix;
        final BitArrayInt[] cliques;

        public TestData(AdjacencyMatrix matrix, BitArrayInt[] cliques) {
            this.matrix = matrix;
            this.cliques = cliques;
        }
    }
}