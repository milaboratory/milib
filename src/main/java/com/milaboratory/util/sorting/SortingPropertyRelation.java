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

/**
 * Describes relation between two properties in respect to equality operation.
 *
 * Compatibility is defined in the following terms:
 *
 * A = this
 * B = other
 *
 * https://en.wikipedia.org/wiki/Necessity_and_sufficiency
 */
public enum SortingPropertyRelation {
    /**
     * Equality or not equality of one property has no connection to the equality or not equality of another property
     */
    None(false, false),
    /**
     * Equality of property A (this) is necessary for the equality of property B (other).
     *
     * In other words:
     * For any pair of o1 and o2, B(o1) == B(o2)  =>  A(o1) == A(o2)
     */
    Necessary(true, false),
    /**
     * Equality of property A (this) is sufficient for the equality of property B (other).
     *
     * In other words:
     * For any pair of o1 and o2, A(o1) == A(o2)  =>  B(o1) == B(o2)
     */
    Sufficient(false, true),
    /**
     * E.g. necessary and sufficient for each others' equality.
     *
     * In other words:
     * For any pair of o1 and o2, B(o1) == B(o2)  <=>  A(o1) == A(o2)
     */
    Equal(true, true);

    final boolean
            necessary,
            sufficient;

    SortingPropertyRelation(boolean necessary, boolean sufficient) {
        this.necessary = necessary;
        this.sufficient = sufficient;
    }
}
