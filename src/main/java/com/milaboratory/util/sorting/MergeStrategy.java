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

import cc.redberry.pipe.OutputPort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class describes the optimal strategy to group objects by a specific set of properties,
 * given an original stream is sorted by a certain sequence of properties.
 *
 * @param <T> type of stream objects
 */
public final class MergeStrategy<T> {
    /**
     * Track changes in the following groups to create super-groups
     */
    public final List<? extends SortingProperty<? super T>> trackChanges;
    /**
     * Additionally group objects inside super-groups using the following groups to achieve desired grouping
     */
    public final List<? extends SortingProperty<? super T>> postGrouping;

    public MergeStrategy(List<? extends SortingProperty<? super T>> trackChanges, List<? extends SortingProperty<? super T>> postGrouping) {
        Objects.requireNonNull(trackChanges);
        Objects.requireNonNull(postGrouping);
        this.trackChanges = trackChanges;
        this.postGrouping = postGrouping;
    }

    public OutputPort<List<T>> group(OutputPort<T> origin) {
        return new GroupingOutputPort<>(MergeStrategy.this, origin);
    }

    <U> MergeStrategy<U> wrapped(Function<U, T> extractor) {
        return new MergeStrategy<>(
                trackChanges.stream()
                        .map(p -> SortingUtil.wrapped(p, extractor))
                        .collect(Collectors.toList()),
                postGrouping.stream()
                        .map(p -> SortingUtil.wrapped(p, extractor))
                        .collect(Collectors.toList()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MergeStrategy)) return false;
        MergeStrategy<?> that = (MergeStrategy<?>) o;
        return trackChanges.equals(that.trackChanges) &&
                postGrouping.equals(that.postGrouping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trackChanges, postGrouping);
    }

    @Override
    public String toString() {
        return "CMergeStrategy{" +
                "trackChanges=" + trackChanges +
                ", postGrouping=" + postGrouping +
                '}';
    }

    /**
     * Calculates optimal CMergeStrategy from the existing stream collation level list,
     * and required grouping / merging field set.
     *
     * @param streamCollationLevels collation levels of a given stream
     * @param targetGrouping        required merge / grouping streams
     * @param <T>                   object type
     * @return optimal merge strategy
     */
    public static <T> MergeStrategy<T> calculateStrategy(
            List<? extends SortingProperty<? super T>> streamCollationLevels,
            List<? extends SortingProperty<? super T>> targetGrouping) {
        List<SortingProperty<? super T>> superGrouping = new ArrayList<>();

        // Cloning target grouping, we will remove elements from it
        targetGrouping = new ArrayList<>(targetGrouping);

        outer:
        for (SortingProperty<? super T> streamLevel : streamCollationLevels) {
            Iterator<? extends SortingProperty<? super T>> it = targetGrouping.iterator();
            while (it.hasNext()) {
                SortingProperty<? super T> target = it.next();
                if (streamLevel.relationTo(target) == SortingPropertyRelation.Equal) {
                    superGrouping.add(streamLevel);
                    it.remove();
                    continue outer;
                }
            }
            for (SortingProperty<? super T> target : targetGrouping) {
                if (streamLevel.relationTo(target) == SortingPropertyRelation.Necessary) {
                    superGrouping.add(streamLevel);
                    continue outer;
                }
            }
            break;
        }

        return new MergeStrategy<T>(superGrouping, (List<SortingProperty<T>>) targetGrouping);
    }
}
