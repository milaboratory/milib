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

import java.util.Comparator;

public abstract class AbstractSortingProperty<T, P> implements SortingProperty<T> {
    @Override
    public final int compare(T o1, T o2) {
        return propertyComparator().compare(get(o1), get(o2));
    }

    public abstract Comparator<P> propertyComparator();

    @Override
    public abstract P get(T obj);

    public static abstract class Natural<T, P extends Comparable<P>> extends AbstractSortingProperty<T, P> {
        @Override
        public Comparator<P> propertyComparator() {
            return Comparator.naturalOrder();
        }
    }
}
