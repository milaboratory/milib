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
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class MergeStrategyTest {
    @Test
    public void substrTest1() {
        Substring ss1 = new Substring(2, 4);
        Substring ss2 = new Substring(2, 5);

        List<String> strings = new ArrayList<>();
        strings.add("ATTAGACA");
        strings.add("GACATATA");
        strings.add("TGCGATGC");
        strings.add("AGCGGAGG");
        strings.add("AGCGGAAA");
        strings.add("AGCGAAAA");
        strings.add("AATTCGAA");

        strings.sort(Comparator.comparing(s -> (String) ss1.get(s)));

        List<Substring> streamGrouping = new ArrayList<>();
        streamGrouping.add(ss1);

        List<Substring> targetGrouping = new ArrayList<>();
        targetGrouping.add(ss2);

        MergeStrategy<String> expectedStrategy = new MergeStrategy<>(
                Collections.singletonList(ss1),
                Collections.singletonList(ss2)
        );

        Assert.assertEquals(expectedStrategy, MergeStrategy.calculateStrategy(streamGrouping, targetGrouping));

        HashSet<List<String>> expectedGroups =
                new HashSet<>(strings.stream().collect(Collectors.groupingBy(ss2::get)).values());

        for (List<String> grp : CUtils.it(expectedStrategy.group(CUtils.asOutputPort(strings))))
            Assert.assertTrue(expectedGroups.remove(grp));

        Assert.assertTrue(expectedGroups.isEmpty());
    }

    public static final class Substring extends AbstractHashSortingProperty.Natural<String, String> {
        final int from, to;

        public Substring(int from, int to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public SortingPropertyRelation relationTo(SortingProperty<?> other) {
            if (other instanceof Substring) {
                int otherFrom = ((Substring) other).from;
                int otherTo = ((Substring) other).to;

                if (from == otherFrom && to == otherTo)
                    return SortingPropertyRelation.Equal;

                if (otherFrom <= from && to <= otherTo)
                    return SortingPropertyRelation.Necessary;

                if (from <= otherFrom && otherTo <= to)
                    return SortingPropertyRelation.Sufficient;
            }
            return SortingPropertyRelation.None;
        }

        @Override
        public String get(String obj) {
            return obj.substring(from, to);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Substring)) return false;
            Substring substring = (Substring) o;
            return from == substring.from &&
                    to == substring.to;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }

        @Override
        public String toString() {
            return "Substring{" +
                    "from=" + from +
                    ", to=" + to +
                    '}';
        }
    }
}