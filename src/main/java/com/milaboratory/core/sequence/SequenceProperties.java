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
package com.milaboratory.core.sequence;

import com.milaboratory.core.Range;
import com.milaboratory.util.sorting.AbstractHashSortingProperty;
import com.milaboratory.util.sorting.AbstractSortingProperty;
import com.milaboratory.util.sorting.SortingProperty;
import com.milaboratory.util.sorting.SortingPropertyRelation;

public final class SequenceProperties {
    private SequenceProperties() {
    }

    private interface Subseq<T> extends SortingProperty<T> {
        Range range();

        @Override
        default SortingPropertyRelation relationTo(SortingProperty<?> other) {
            Range ourRange = range();

            if (other instanceof Subseq) {
                Range otherRange = ((Subseq) other).range();

                if (ourRange.equals(otherRange))
                    return SortingPropertyRelation.Equal;

                if (otherRange.contains(ourRange))
                    return SortingPropertyRelation.Necessary;

                if (ourRange.contains(otherRange))
                    return SortingPropertyRelation.Sufficient;
            }

            return SortingPropertyRelation.None;
        }
    }

    public static final class HSubsequence<S extends Sequence<S>>
            extends AbstractHashSortingProperty.Natural<S, S>
            implements Subseq<S> {
        private final Range range;

        public HSubsequence(Range range) {
            this.range = range;
        }

        @Override
        public Range range() {
            return range;
        }

        @Override
        public S get(S obj) {
            return obj.size() < range.getUpper() ? obj.getRange(0, 0) : obj.getRange(range);
        }
    }

    public static final class Subsequence<S extends Sequence<S>>
            extends AbstractSortingProperty.Natural<S, S>
            implements Subseq<S> {
        private final Range range;

        public Subsequence(Range range) {
            this.range = range;
        }

        @Override
        public Range range() {
            return range;
        }

        @Override
        public S get(S obj) {
            return obj.size() < range.getUpper() ? obj.getRange(0, 0) : obj.getRange(range);
        }
    }
}
