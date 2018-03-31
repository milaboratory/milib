/*
 * Copyright 2018 MiLaboratory.com
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

import com.milaboratory.core.sequence.SequenceQuality;

public final class QualityTrimmer {
    private QualityTrimmer() {
    }

    /**
     * Core trimming method. Implements main algorithm that finds optimal trimming position.
     *
     * @param quality                 sequence quality
     * @param leftmostPosition        scanning region from, inclusive
     * @param rightmostPosition       scanning region to, exclusive
     * @param scanIncrement           +1 to scan to the right (trim on the left side of the sequence);
     *                                -1 to scan to the left (trim on the right side of the sequence)
     * @param windowSize              scanning window size
     * @param searchForRise           search mode; if true - searches for beginning of a "good quality region"
     *                                (e.g. useful for trimming of sequencing reads from sides);
     *                                if false - searches for the end of "good quality regions"
     *                                (e.g. useful to trim-off low quality leftovers from assembled contig)
     * @param averageQualityThreshold target minimal average quality
     * @return trimming position if search was successful (last position of the region) or
     * (-2 - trimming position) if search was unsuccessful
     */
    public static int trim(SequenceQuality quality,
                           int leftmostPosition, int rightmostPosition, int scanIncrement,
                           int windowSize,
                           boolean searchForRise, float averageQualityThreshold) {
        if (quality.size() == 0)
            return -1;

        if (scanIncrement != -1 && scanIncrement != 1)
            throw new IllegalArgumentException("Wrong value for scanIncrement.");

        // Number of iterations
        int positionsToScan = rightmostPosition - leftmostPosition;

        // Trimming window size if scanning region is too small
        windowSize = Math.min(positionsToScan, windowSize);

        // Minimal sum quality for the window
        int sumThreshold = (int) Math.ceil(averageQualityThreshold * windowSize);

        // Tracks current sum of quality scores inside the window
        int sum = 0;

        // Current position
        int position = scanIncrement == 1 ? leftmostPosition : rightmostPosition - 1;
        // Last position of current search window
        int windowEndPosition = position;

        // Calculating initial sum quality value
        for (int i = 0; i < windowSize; i++) {
            sum += quality.value(position);
            position += scanIncrement;
        }

        // Main search pass
        while ((searchForRise ^ (sum >= sumThreshold)) && // if searchForRise == true, the loop will be terminated on the first position where sum >= sumThreshold
                position >= leftmostPosition &&
                position < rightmostPosition) {
            sum -= quality.value(windowEndPosition);
            sum += quality.value(position);
            windowEndPosition += scanIncrement;
            position += scanIncrement;
        }

        // Determine whether the search was successful
        if (searchForRise ^ (sum >= sumThreshold)) { // If this condition is still true, search was unsuccessful
            // One step back
            position -= scanIncrement;
            return -2 - position;
        }

        // Successful search

        // Searching for actual boundary of the region
        do {
            position -= scanIncrement;
        } while (position >= leftmostPosition &&
                position < rightmostPosition &&
                (searchForRise ^ (quality.value(position) < averageQualityThreshold)));

        // assert scanIncrement == 1 ? position >= windowEndPosition : position <= windowEndPosition;

        return position;
    }
}
