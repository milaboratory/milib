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
import org.junit.Assert;
import org.junit.Test;

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
}