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

import com.milaboratory.util.BitArray;
import com.milaboratory.util.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class AdjacencyMatrix {
    private final BitArray[] data;
    private final int size;

    public AdjacencyMatrix(int size) {
        this.size = size;
        this.data = new BitArray[size];
        for (int i = 0; i < size; i++)
            this.data[i] = new BitArray(size);
    }

    public void setConnected(int i, int j) {
        this.data[i].set(j);
        this.data[j].set(i);
    }

    /**
     * Extract maximal cliques using Bron–Kerbosch algorithm.
     *
     * @return list of maximal cliques
     */
    public List<BitArray> calculateMaximalCliques() {
        BStack stack = new BStack();
        return null;
    }

    final class BStack {
        final BList R = new BList();
        final BList X = new BList();
        final BList P = new BList();
        final IntArrayList lastP = new IntArrayList();
        int currentLevel = 0;

        public BStack() {
            lastP.push(0);
        }

        BitArray currentR() {
            return R.get(currentLevel);
        }

        BitArray currentX() {
            return X.get(currentLevel);
        }

        BitArray currentP() {
            return P.get(currentLevel);
        }

        int nextVertex() {
            //int nextVertex = currentP().getBits();
            return 0;
        }

        BitArray nextR() {
            return R.get(currentLevel + 1);
        }

        BitArray nextX() {
            return X.get(currentLevel + 1);
        }

        BitArray nextP() {
            return P.get(currentLevel + 1);
        }

        void push() {
            currentLevel++;
            lastP.push(0);
        }

        boolean pop() {
            currentLevel--;
            lastP.pop();
            return currentLevel != -1;
        }
    }

    final class BList {
        final List<BitArray> list = new ArrayList<>();

        public BitArray get(int i) {
            if (i >= list.size())
                for (int j = list.size(); j < i; j++)
                    list.add(new BitArray(size));
            return list.get(i);
        }
    }
}
