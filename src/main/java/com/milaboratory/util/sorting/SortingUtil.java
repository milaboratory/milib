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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public final class SortingUtil {
    private SortingUtil() {
    }

    public static <T> Comparator<T> combine(Comparator<? super T>[] comparators) {
        return SortingUtil.<T>combine(Arrays.asList(comparators));
    }

    public static <T> Comparator<T> combine(List<? extends Comparator<? super T>> comparators) {
        if (comparators.size() == 0)
            throw new IllegalArgumentException();
        Comparator<T> comparator = (Comparator) comparators.get(0);
        for (int i = 1; i < comparators.size(); i++)
            comparator = comparator.thenComparing(comparators.get(i));
        return comparator;
    }

    static <T, E> SortingProperty<T> wrapped(SortingProperty<? super E> elementProperty, Function<T, E> extractor) {
        return new SortingProperty<T>() {
            @Override
            public Object get(T obj) {
                return elementProperty.get(extractor.apply(obj));
            }

            @Override
            public int compare(T o1, T o2) {
                return elementProperty.compare(extractor.apply(o1), extractor.apply(o2));
            }
        };
    }
}
