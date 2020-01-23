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
package com.milaboratory.core.alignment;

import com.milaboratory.core.Range;

import java.util.Objects;

public final class AlignmentElement {
    final Range referenceRange, queryRange;
    final boolean isMatch;

    public AlignmentElement(Range referenceRange, Range queryRange, boolean isMatch) {
        Objects.requireNonNull(referenceRange);
        Objects.requireNonNull(queryRange);
        if (referenceRange.length() > 0 && queryRange.length() > 0 && queryRange.length() != referenceRange.length())
            throw new IllegalArgumentException();
        this.referenceRange = referenceRange;
        this.queryRange = queryRange;
        this.isMatch = isMatch;
    }

    public AlignmentElementType getType() {
        if (referenceRange.isEmpty())
            return AlignmentElementType.Insertion;
        if (queryRange.isEmpty())
            return AlignmentElementType.Deletion;
        return isMatch ? AlignmentElementType.Match : AlignmentElementType.Mismatch;
    }

    public int getCigarLength() {
        return Math.max(referenceRange.length(), queryRange.length());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AlignmentElement)) return false;
        AlignmentElement that = (AlignmentElement) o;
        return isMatch == that.isMatch &&
                referenceRange.equals(that.referenceRange) &&
                queryRange.equals(that.queryRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referenceRange, queryRange, isMatch);
    }

    @Override
    public String toString() {
        return "" + referenceRange + " " + getCigarLength() + getType().cigarLetter + " " + queryRange;
    }
}
