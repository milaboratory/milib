/*
 * Copyright 2015 MiLaboratory.com
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
package com.milaboratory.util;

import com.milaboratory.core.Range;

import java.util.Comparator;
import java.util.TreeMap;

import static java.util.Map.Entry;

public final class RangeMap<T> {
    private final TreeMap<Range, T> map = new TreeMap<>(rangeComparator);

    public RangeMap() {
    }

    public Entry<Range, T> getEntryThatContains(Range range) {
        Entry<Range, T> ret = map.floorEntry(range);
        if (ret != null && ret.getKey().contains(range))
            return ret;
        return null;
    }

    public void put(Range range, T value) {
        if (findSingleIntersection(range) != null)
            throw new IntersectingRangesException();
        map.put(range, value);
    }

    public T remove(Range range) {
        return map.remove(range);
    }

    public Entry<Range, T> findSingleIntersection(Range range) {
        Entry<Range, T> tmp = map.floorEntry(range);
        Entry<Range, T> ret = null;
        if (tmp != null && tmp.getKey().intersectsWith(range))
            ret = tmp;
        tmp = map.higherEntry(range);
        if (tmp != null && tmp.getKey().intersectsWith(range))
            if (ret != null)
                throw new IllegalArgumentException("Several intersection hits");
            else
                ret = tmp;
        return ret;
    }

    public static final class IntersectingRangesException extends RuntimeException {
        public IntersectingRangesException() {
        }

        public IntersectingRangesException(String message) {
            super(message);
        }
    }

    private static final Comparator<Range> rangeComparator = new Comparator<Range>() {
        @Override
        public int compare(Range o1, Range o2) {
            int cmp;
            if ((cmp = Integer.compare(o1.getLower(), o2.getLower())) != 0)
                return cmp;

            return Integer.compare(o2.getUpper(), o1.getUpper());
        }
    };
}
