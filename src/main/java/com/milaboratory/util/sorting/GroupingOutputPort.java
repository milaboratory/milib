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
import cc.redberry.pipe.OutputPortCloseable;

import java.util.*;
import java.util.stream.Collectors;

public final class GroupingOutputPort<T> implements OutputPortCloseable<List<T>> {
    private final Object mutex = new Object();
    private T lastObject;
    private final OutputPort<T> innerOutputPort;
    private final MergeStrategy<T> groupingStrategy;
    private final Queue<List<T>> queue = new ArrayDeque<>();

    public GroupingOutputPort(MergeStrategy<T> mergeStrategy, OutputPort<T> targetOutputPort) {
        this.innerOutputPort = targetOutputPort;
        this.groupingStrategy = mergeStrategy;
    }

    private List<Object> extractSuperGroupValues(T obj) {
        return groupingStrategy.streamGrouping.stream()
                .map(prop -> prop.get(obj))
                .collect(Collectors.toList());
    }

    private int compareBySuperGroup(T o1, T o2) {
        int compare;
        for (int i = 0; i < groupingStrategy.streamGrouping.size(); i++) {
            SortingProperty<? super T> sortingProperty = groupingStrategy.streamGrouping.get(i);
            if ((compare = sortingProperty.compare(o1, o2)) != 0)
                return compare;
        }

        return 0;
    }

    private List<Object> extractPostGroupValues(T obj) {
        return groupingStrategy.postGrouping.stream()
                .map(prop -> prop.get(obj))
                .collect(Collectors.toList());
    }

    private final class PostGroupsComparator implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            int compare;
            for (int i = 0; i < groupingStrategy.postGrouping.size(); i++) {
                SortingProperty<? super T> grp = groupingStrategy.postGrouping.get(i);
                if ((compare = grp.compare(o1, o2)) != 0)
                    return compare;
            }

            return 0;
        }
    }

    /**
     * Scans one super-group and fills the queue with groups from this super-group
     *
     * @return true if operation was successful or false if there are no more objects
     * in the underlying iterator
     */
    private boolean fillQueue() {
        if (!queue.isEmpty())
            throw new IllegalStateException("Queue not empty.");

        // innerOutputPort was empty from the very beginning
        // or call to take() after the whole object was closed
        if (lastObject == null
                && (lastObject = innerOutputPort.take()) == null) // this loads value from innerOutputPort if lastObject == null
            return false;

        T keyObject = this.lastObject;

        if (groupingStrategy.postGrouping.isEmpty()) {
            List<T> group = new ArrayList<>();
            group.add(keyObject);
            this.lastObject = null;

            T next;
            while ((next = innerOutputPort.take()) != null) {
                int cmp = compareBySuperGroup(keyObject, next);
                if (cmp > 0)
                    throw new IllegalArgumentException("Input port soring is not compatible " +
                            "with the provided grouping strategy");
                if (cmp != 0) {
                    this.lastObject = next;
                    break;
                }
                group.add(next);
            }

            queue.add(group);
        } else {
            // postGroupingValues -> group
            SortedMap<T, List<T>> groups = new TreeMap<>(new PostGroupsComparator());
            groups.put(lastObject,
                    new ArrayList<>(Collections.singletonList(lastObject)));
            lastObject = null;

            T next;
            while ((next = innerOutputPort.take()) != null) {
                int cmp = compareBySuperGroup(keyObject, next);
                if (cmp > 0)
                    throw new IllegalArgumentException("Input port soring is not compatible " +
                            "with the provided grouping strategy");
                if (cmp != 0) {
                    lastObject = next;
                    break;
                }

                groups.computeIfAbsent(
                        next,
                        __ -> new ArrayList<>()
                ).add(next);
            }

            queue.addAll(groups.values());
        }

        return true;
    }

    @Override
    public List<T> take() {
        synchronized (mutex) {
            while (queue.isEmpty())
                if (!fillQueue())
                    return null;
            return queue.remove();
        }
    }

    @Override
    public void close() {
        synchronized (mutex) {
            if (innerOutputPort instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) innerOutputPort).close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
