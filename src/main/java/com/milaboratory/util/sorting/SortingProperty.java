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

/**
 * Represents a property a sequence of objects can be sorted by.
 *
 * @param <T> type of objects in stream
 */
public interface SortingProperty<T> extends Comparator<T> {
    /**
     * Returns relation to other property. See docs for {@link SortingPropertyRelation} for details.
     *
     * @param other other property defined for the same object type
     * @return relation to other property
     */
    default SortingPropertyRelation relationTo(SortingProperty<?> other) {
        return this.equals(other) ? SortingPropertyRelation.Equal : SortingPropertyRelation.None;
    }

    /**
     * Extracts the property value
     *
     * @param obj target object
     * @return property value
     */
    Object get(T obj);
}