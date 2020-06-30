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

    private List<Object> extractPostGroupValues(T obj) {
        return groupingStrategy.postGrouping.stream()
                .map(prop -> prop.get(obj))
                .collect(Collectors.toList());
    }

    /**
     * Scans one super-group and fills the queue with groups from this super-group
     *
     * @return true if operation was successful or false if there are no more objects
     * in the underlying iterator
     */
    private boolean fillQueue() {
        assert queue.isEmpty();

        if (lastObject == null
                && (lastObject = innerOutputPort.take()) == null)
            return false;

        List<Object> currentSGV = extractSuperGroupValues(lastObject);

        if (groupingStrategy.postGrouping.isEmpty()) {
            List<T> group = new ArrayList<>();
            group.add(lastObject);
            lastObject = null;

            T next;
            while ((next = innerOutputPort.take()) != null) {
                if (!extractSuperGroupValues(next).equals(currentSGV)) {
                    lastObject = next;
                    break;
                }
                group.add(next);
            }

            queue.add(group);
        } else {
            // postGroupingValues -> group
            Map<List<Object>, List<T>> groups = new HashMap<>();
            groups.put(extractPostGroupValues(lastObject),
                    new ArrayList<>(Collections.singletonList(lastObject)));
            lastObject = null;

            T next;
            while ((next = innerOutputPort.take()) != null) {

                if (!extractSuperGroupValues(next).equals(currentSGV)) {
                    lastObject = next;
                    break;
                }

                groups.computeIfAbsent(
                        extractPostGroupValues(next),
                        __ -> new ArrayList<>()
                ).add(next);
            }

            queue.addAll(groups.values());
        }

        return true;
    }

    @Override
    public List<T> take() {
        while (queue.isEmpty())
            if (!fillQueue())
                return null;
        return queue.remove();
    }

    @Override
    public void close() {
        if (innerOutputPort instanceof AutoCloseable) {
            try {
                ((AutoCloseable) innerOutputPort).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
