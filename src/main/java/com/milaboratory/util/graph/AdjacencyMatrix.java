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
import com.milaboratory.util.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class AdjacencyMatrix {
    private final BitArrayInt[] data;
    private final int size;

    public AdjacencyMatrix(int size) {
        this.size = size;
        this.data = new BitArrayInt[size];
        for (int i = 0; i < size; i++)
            this.data[i] = new BitArrayInt(size);
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
    public List<BitArrayInt> calculateMaximalCliques() {
        List<BitArrayInt> cliques = new ArrayList<>();
        BStack stack = new BStack();
        stack.currentP().setAll();
        stack.currentPi().setAll();
        BitArrayInt tmp = new BitArrayInt(size);

        while (true) {
            int v = stack.nextVertex();

            if (v == -1)
                if (stack.pop())
                    continue;
                else
                    break;

            stack.currentP().clear(v);

            stack.loadAndGetNextR().set(v);
            stack.loadAndGetNextP().and(data[v]);
            stack.loadAndGetNextX().and(data[v]);

            stack.currentX().set(v);

            if (stack.nextP().isEmpty() && stack.nextX().isEmpty()){
                cliques.add(stack.nextR().clone());
                continue;
            }

            tmp.loadValueFrom(stack.nextP());
            tmp.or(stack.nextX());
            int u = 0, bestU = -1, count = -1;
            while ((u = tmp.nextBit(u)) != -1) {
                int c = tmp.numberOfCommonBits(data[u]);
                if (bestU == -1 || c > count) {
                    bestU = u;
                    count = c;
                }
                ++u;
            }

            stack.nextPi().loadValueFrom(data[bestU]);
            stack.nextPi().clear(bestU);
            stack.nextPi().xor(stack.nextP());
            stack.nextPi().and(stack.nextP());

            stack.push();
        }
        return cliques;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++)
            builder.append(data[i]).append('\n');
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    final class BStack {
        final BList R = new BList();
        final BList X = new BList();
        final BList P = new BList();
        /**
         * P for vertices enumeration = P / N(v)
         */
        final BList Pi = new BList();
        final IntArrayList lastVertex = new IntArrayList();
        int currentLevel = 0;

        public BStack() {
            lastVertex.push(0);
        }

        BitArrayInt currentR() {
            return R.get(currentLevel);
        }

        BitArrayInt currentX() {
            return X.get(currentLevel);
        }

        BitArrayInt currentP() {
            return P.get(currentLevel);
        }

        BitArrayInt currentPi() {
            return Pi.get(currentLevel);
        }

        int nextVertex() {
            int nextVertex = currentPi().nextBit(lastVertex.peek());
            lastVertex.set(currentLevel, nextVertex + 1);
            return nextVertex;
        }

        BitArrayInt loadAndGetNextR() {
            R.get(currentLevel + 1).loadValueFrom(R.get(currentLevel));
            return R.get(currentLevel + 1);
        }

        BitArrayInt loadAndGetNextX() {
            X.get(currentLevel + 1).loadValueFrom(X.get(currentLevel));
            return X.get(currentLevel + 1);
        }

        BitArrayInt loadAndGetNextP() {
            P.get(currentLevel + 1).loadValueFrom(P.get(currentLevel));
            return P.get(currentLevel + 1);
        }

        BitArrayInt nextX() {
            return X.get(currentLevel + 1);
        }

        BitArrayInt nextP() {
            return P.get(currentLevel + 1);
        }

        BitArrayInt nextR() {
            return R.get(currentLevel + 1);
        }

        BitArrayInt nextPi() {
            return Pi.get(currentLevel + 1);
        }

        void push() {
            currentLevel++;
            lastVertex.push(0);
        }

        boolean pop() {
            currentLevel--;
            lastVertex.pop();
            return currentLevel != -1;
        }
    }

    final class BList {
        final List<BitArrayInt> list = new ArrayList<>();

        public BitArrayInt get(int i) {
            if (i >= list.size())
                for (int j = list.size(); j <= i; j++)
                    list.add(new BitArrayInt(size));
            return list.get(i);
        }
    }
}
