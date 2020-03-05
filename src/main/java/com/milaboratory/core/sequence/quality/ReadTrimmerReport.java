/*
 * Copyright 2019 MiLaboratory, LLC
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
package com.milaboratory.core.sequence.quality;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.milaboratory.core.Range;
import com.milaboratory.core.io.sequence.SequenceRead;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class ReadTrimmerReport implements ReadTrimmerListener {
    final AtomicLong totalAlignments = new AtomicLong();
    final AtomicLongArray bySideEvents = new AtomicLongArray(4);
    final AtomicLongArray bySideNucleotides = new AtomicLongArray(4);

    public long getAlignments() {
        return totalAlignments.get();
    }

    @JsonProperty("r1LeftTrimmedEvents")
    public long getR1LeftTrimmedEvents() {
        return bySideEvents.get(0);
    }

    public void setR1LeftTrimmedEvents(long v) {
        bySideEvents.set(0, v);
    }

    @JsonProperty("r1RightTrimmedEvents")
    public long getR1RightTrimmedEvents() {
        return bySideEvents.get(1);
    }

    public void setR1RightTrimmedEvents(long v) {
        bySideEvents.set(1, v);
    }

    @JsonProperty("r2LeftTrimmedEvents")
    public long getR2LeftTrimmedEvents() {
        return bySideEvents.get(2);
    }

    public void setR2LeftTrimmedEvents(long v) {
        bySideEvents.set(2, v);
    }

    @JsonProperty("r2RightTrimmedEvents")
    public long getR2RightTrimmedEvents() {
        return bySideEvents.get(3);
    }

    public void setR2RightTrimmedEvents(long v) {
        bySideEvents.set(3, v);
    }

    @JsonProperty("r1LeftTrimmedNucleotides")
    public long getR1LeftTrimmedNucleotides() {
        return bySideNucleotides.get(0);
    }

    public void setR1LeftTrimmedNucleotides(long v) {
        bySideNucleotides.set(0, v);
    }

    @JsonProperty("r1RightTrimmedNucleotides")
    public long getR1RightTrimmedNucleotides() {
        return bySideNucleotides.get(1);
    }

    public void setR1RightTrimmedNucleotides(long v) {
        bySideNucleotides.set(1, v);
    }

    @JsonProperty("r2LeftTrimmedNucleotides")
    public long getR2LeftTrimmedNucleotides() {
        return bySideNucleotides.get(2);
    }

    public void setR2LeftTrimmedNucleotides(long v) {
        bySideNucleotides.set(2, v);
    }

    @JsonProperty("r2RightTrimmedNucleotides")
    public long getR2RightTrimmedNucleotides() {
        return bySideNucleotides.get(3);
    }

    public void setR2RightTrimmedNucleotides(long v) {
        bySideNucleotides.set(3, v);
    }

    @Override
    public void onSequence(SequenceRead originalRead, int readIndex, Range range, boolean trimmed) {
        if (readIndex == 0)
            totalAlignments.incrementAndGet();

        int originalLength = originalRead.getRead(readIndex).getData().size();

        if (range == null) {
            bySideEvents.incrementAndGet(readIndex * 2);
            bySideEvents.incrementAndGet(readIndex * 2 + 1);
            bySideNucleotides.addAndGet(readIndex * 2, originalLength / 2);
            bySideNucleotides.addAndGet(readIndex * 2, originalLength - (originalLength / 2));
        } else {
            if (range.getLower() > 0) {
                bySideEvents.incrementAndGet(readIndex * 2);
                bySideNucleotides.addAndGet(readIndex * 2, range.getLower());
            }
            if (range.getUpper() < originalLength) {
                bySideEvents.incrementAndGet(readIndex * 2 + 1);
                bySideNucleotides.addAndGet(readIndex * 2, originalLength - range.getUpper());
            }
        }
    }
}
