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
import java.util.function.Function;

/**
 * Merges several sorted output ports, into a single sorted output port.
 *
 * Equal objects (in terms of provided comparator), will be outputted in the same order the originating
 * output ports were provided to the constructor of this instance.
 */
public final class MergingOutputPort<T> implements OutputPortCloseable<T> {
    private final Object mutex = new Object();
    private final Comparator<T> comparator;
    private final SortedSet<OPWrapper> ports;
    private volatile int lastPortIndex = -1;

    public MergingOutputPort(Comparator<T> comparator,
                             List<OutputPort<T>> outputPorts) {
        this.comparator = comparator;

        this.ports = new TreeSet<>(Comparator
                .<OPWrapper, T>comparing(opw -> opw.nextValue, comparator)
                .thenComparing(opw -> opw.index));

        for (int i = 0; i < outputPorts.size(); i++) {
            OPWrapper opw = new OPWrapper(i, outputPorts.get(i));
            if (opw.hasNextValue()) // Not empty port
                ports.add(opw);
        }
    }

    @Override
    public T take() {
        synchronized (mutex) {
            if (ports.isEmpty()) {
                lastPortIndex = -1;
                return null;
            }
            OPWrapper op = ports.first();
            ports.remove(op);
            T value = op.nextValue;
            lastPortIndex = op.index;
            if (op.advance())
                ports.add(op);
            return value;
        }
    }

    /**
     * Returns the output port that will wrap each of the output objects produced by
     * this port with the index of the original output port it was produced by.
     */
    public OutputPortCloseable<WithIndex<T>> indexed() {
        return new OutputPortCloseable<WithIndex<T>>() {
            @Override
            public void close() {
                MergingOutputPort.this.close();
            }

            @Override
            public WithIndex<T> take() {
                synchronized (mutex) {
                    T obj = MergingOutputPort.this.take();
                    return obj == null ? null : new WithIndex<>(MergingOutputPort.this.lastPortIndex, obj);
                }
            }
        };
    }

    @Override
    public void close() {
        synchronized (mutex) {
            try {
                for (OPWrapper port : ports)
                    if (port.port instanceof AutoCloseable)
                        ((AutoCloseable) port.port).close();
                ports.clear();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final class OPWrapper {
        /**
         * Index of the output port, used for last sorting value
         */
        final int index;
        final OutputPort<T> port;
        T nextValue;

        OPWrapper(int index, OutputPort<T> port) {
            this.index = index;
            this.port = port;
            advance();
        }

        boolean advance() {
            T next = port.take();
            if (next != null && nextValue != null && comparator.compare(nextValue, next) > 0)
                throw new IllegalArgumentException("Output port not sorted");
            this.nextValue = next;
            return this.nextValue != null;
        }

        boolean hasNextValue() {
            return nextValue != null;
        }
    }

    public static final class WithIndex<T> {
        public final int index;
        public final T obj;

        public WithIndex(int index, T obj) {
            this.index = index;
            this.obj = obj;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WithIndex<?> withIndex = (WithIndex<?>) o;
            return index == withIndex.index &&
                    obj.equals(withIndex.obj);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, obj);
        }

        @Override
        public String toString() {
            return "WithIndex{" +
                    "index=" + index +
                    ", obj=" + obj +
                    '}';
        }
    }

    private static final Function<WithIndex<?>, ?> WI_EXTRACTOR = withIndex -> withIndex.obj;

    public static <T> Function<WithIndex<T>, T> withIndexUnWrapper() {
        return (Function) WI_EXTRACTOR;
    }

    /**
     * Performs stream join operation on the list of sorted output ports.
     *
     * Uses MergeStrategy, that can be calculated for the given streams ordering and required merge "keys".
     */
    public static <T> OutputPortCloseable<List<List<T>>> join(MergeStrategy<T> strategy, List<OutputPort<T>> ports) {
        OutputPortCloseable<WithIndex<T>> indexed = new MergingOutputPort<>(SortingUtil.combine(strategy.streamGrouping), ports).indexed();
        GroupingOutputPort<WithIndex<T>> wiGrouped = new GroupingOutputPort<>(strategy.wrapped(withIndexUnWrapper()), indexed);
        return new OutputPortCloseable<List<List<T>>>() {
            @Override
            public void close() {
                wiGrouped.close();
            }

            @Override
            public List<List<T>> take() {
                List<WithIndex<T>> grp = wiGrouped.take();
                if (grp == null)
                    return null;

                List<T>[] row = new List[ports.size()];
                for (WithIndex<T> wi : grp) {
                    List<T> col = row[wi.index];
                    if (col == null)
                        row[wi.index] = col = new ArrayList<>(1);
                    col.add(wi.obj);
                }

                for (int i = 0; i < ports.size(); i++)
                    if (row[i] == null)
                        row[i] = Collections.EMPTY_LIST;

                return Arrays.asList(row);
            }
        };
    }

    public static final class JoinRow<T> {
        public List<List<T>> row;
    }
}
