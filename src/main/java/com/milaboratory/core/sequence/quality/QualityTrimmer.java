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

import com.milaboratory.core.Range;
import com.milaboratory.core.sequence.SequenceQuality;

import java.util.ArrayList;
import java.util.List;

/**
 * Searches for a region where for the given threshold and window size:
 *
 * 1. For any window of a predefined size inside the region average quality is greater than the threshold
 * 2. Edge nucleotide position quality is greater than the threshold
 */
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
     * @param searchForRise           search mode; if true - searches for beginning of a "good quality region"
     *                                (e.g. useful for trimming of sequencing reads from sides);
     *                                if false - searches for the end of "good quality regions"
     *                                (e.g. useful to trim-off low quality leftovers from assembled contig)
     * @param averageQualityThreshold target minimal average quality
     * @param windowSize              scanning window size
     * @return trimming position if search was successful (last position of the region) or
     * (-2 - trimming position) if search was unsuccessful
     */
    public static int trim(SequenceQuality quality,
                           int leftmostPosition, int rightmostPosition, int scanIncrement,
                           boolean searchForRise,
                           float averageQualityThreshold, int windowSize) {
        if (quality.size() == 0)
            return scanIncrement == 1 ? -1 : 0;

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

        // Checking whether the criteria #1 is NOT met for the first window
        if (searchForRise == (sum >= sumThreshold)) {
            // Trying to rewind windows by moving outside the search region
            for (int i = 0; i < windowSize; i++) {
                windowEndPosition -= scanIncrement;
                position -= scanIncrement;
                if (windowEndPosition < 0 || windowEndPosition >= quality.size()) // Failed to find window meeting the criteria #1
                    return (scanIncrement == 1 ? leftmostPosition : rightmostPosition - 1) - scanIncrement;
                sum += quality.value(windowEndPosition);
                sum -= quality.value(position);
                if (searchForRise ^ (sum >= sumThreshold)) {
                    // Final pass for criteria #2
                    while ((searchForRise ^ (quality.value(position) < averageQualityThreshold))
                            && i < windowSize) {
                        position -= scanIncrement;
                        ++i;
                    }
                    return position;
                }
            }
            return position - scanIncrement;
        }

        // Main search pass (criteria #1)
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

        // Searching for actual boundary of the region, reverse search (criteria #2)
        do {
            position -= scanIncrement;
        } while (position >= leftmostPosition &&
                position < rightmostPosition &&
                (searchForRise ^ (quality.value(position) < averageQualityThreshold)));

        // assert scanIncrement == 1 ? position >= windowEndPosition : position <= windowEndPosition;

        return position;
    }


    /**
     * Core trimming method. Implements main algorithm that finds optimal trimming position.
     *
     * @param quality           sequence quality
     * @param leftmostPosition  scanning region from, inclusive
     * @param rightmostPosition scanning region to, exclusive
     * @param scanIncrement     +1 to scan to the right (trim on the left side of the sequence);
     *                          -1 to scan to the left (trim on the right side of the sequence)
     * @param searchForRise     search mode; if true - searches for beginning of a "good quality region"
     *                          (e.g. useful for trimming of sequencing reads from sides);
     *                          if false - searches for the end of "good quality regions"
     *                          (e.g. useful to trim-off low quality leftovers from assembled contig)
     * @param parameters        trimming parameters
     * @return trimming position if search was successful (last position of the region) or
     * (-2 - trimming position) if search was unsuccessful
     */
    public static int trim(SequenceQuality quality,
                           int leftmostPosition, int rightmostPosition, int scanIncrement,
                           boolean searchForRise,
                           QualityTrimmerParameters parameters) {
        return trim(quality, leftmostPosition, rightmostPosition, scanIncrement, searchForRise, parameters.getAverageQualityThreshold(), parameters.getWindowSize());
    }

    /**
     * Returns absolute position value.
     *
     * position >= -1 ? position : -2 -position
     *
     * @param position
     * @return
     */
    public static int pabs(int position) {
        return position >= -1 ? position : -2 - position;
    }

    /**
     * Extend initialRange to the biggest possible range that fulfils the criteria of QualityTrimmer along the whole extended region.
     *
     * The criteria may not be fulfilled for the initial range.
     *
     * @param quality      quality values
     * @param parameters   trimming parameters
     * @param initialRange initial range to extend
     * @return
     */
    public static Range extendRange(SequenceQuality quality, QualityTrimmerParameters parameters, Range initialRange) {
        int lower = pabs(trim(quality, 0, initialRange.getLower(), -1, false, parameters));
        int upper = pabs(trim(quality, initialRange.getUpper(), quality.size(), +1, false, parameters)) + 1;
        return new Range(lower, upper, initialRange.isReverse());
    }

    /**
     * Find all good quality islands in terms of two criteria listed above.
     *
     * @param quality    quality values
     * @param parameters trimming parameters
     */
    public static Range[] calculateAllIslands(SequenceQuality quality, QualityTrimmerParameters parameters) {
        ArrayList<Range> ranges = new ArrayList<>();
        findIslands(quality, parameters, 0, +1, false, ranges);
        return ranges.toArray(new Range[0]);
    }

    /** Used for tests */
    static Range[] findIslands(SequenceQuality quality, QualityTrimmerParameters parameters,
                               int from, int direction,
                               boolean isReversed) {
        ArrayList<Range> ranges = new ArrayList<>();
        findIslands(quality, parameters, from, direction, isReversed, ranges);
        return ranges.toArray(new Range[0]);
    }

    /**
     * Searches for good quality islands in terms of two criteria listed above.
     *
     * @param quality    quality values
     * @param parameters trimming parameters
     * @param from       initial scan position
     * @param direction  search direction
     * @param isReversed should the resulting ranges be reversed
     * @param ranges     list to add results to (ranges will be added sorted by position)
     */
    static void findIslands(SequenceQuality quality, QualityTrimmerParameters parameters,
                            int from, int direction,
                            boolean isReversed,
                            List<Range> ranges) {
        while (from >= 0 && from < quality.size()) {
            // from supposed to be low quality position
            int islandStart = trim(quality,
                    direction == +1 ? from : 0,
                    direction == +1 ? quality.size() : from,
                    direction, true, parameters);

            if (islandStart < -1)
                // No more good quality islands
                break;

            // Searching for the island boundary
            int islandEnd = pabs(trim(quality,
                    direction == +1 ? islandStart + direction : 0,
                    direction == +1 ? quality.size() : islandStart + direction,
                    direction, false, parameters));

            if (direction == +1)
                ranges.add(new Range(islandStart + 1, islandEnd + 1, isReversed));
            else
                ranges.add(0, new Range(islandEnd, islandStart, isReversed));

            from = islandEnd + direction;
        }
    }

    /**
     * Extend initialRange to the biggest possible range that fulfils the criteria of QualityTrimmer along the whole
     * extended region, then splits leftover ranges into good quality islands.
     *
     * The criteria may not be fulfilled for the initial range.
     *
     * @param quality      quality values
     * @param parameters   trimming parameters
     * @param initialRange initial range to extend
     * @return array of ranges including extended initial range
     */
    public static Range[] calculateIslandsFromInitialRange(SequenceQuality quality, QualityTrimmerParameters parameters, Range initialRange) {
        int lowerInitial = pabs(trim(quality, 0, initialRange.getLower(), -1, false, parameters));
        int upperInitial = pabs(trim(quality, initialRange.getUpper(), quality.size(), +1, false, parameters)) + 1;
        ArrayList<Range> ranges = new ArrayList<>();
        ranges.add(new Range(lowerInitial, upperInitial, initialRange.isReverse()));
        if (lowerInitial > parameters.getWindowSize() - 1)
            findIslands(quality, parameters, lowerInitial - 1, -1, initialRange.isReverse(), ranges);
        if (upperInitial <= quality.size() - parameters.getWindowSize())
            findIslands(quality, parameters, upperInitial, +1, initialRange.isReverse(), ranges);
        return ranges.toArray(new Range[0]);
    }

    /**
     * Trims the quality string by cutting off low quality nucleotides on both edges.
     *
     * The criteria of QualityTrimmer may not be fulfilled for the resulting range. This method detects
     * beginning of the region with stably high quality, once the beginning of the region is detected the algorithm
     * stops. Detected stop positions are "from" and "to" of the output range.
     *
     * @param quality    quality values
     * @param parameters trimming parameters
     * @return trimmed range
     */
    public static Range trim(SequenceQuality quality, QualityTrimmerParameters parameters) {
        return trim(quality, parameters, new Range(0, quality.size()));
    }

    /**
     * Trims the initialRange by cutting off low quality nucleotides on both edges.
     *
     * The criteria of QualityTrimmer may not be fulfilled for the resulting range. This method detects
     * beginning of the region with stably high quality, once the beginning of the region is detected the algorithm
     * stops. Detected stop positions are "from" and "to" of the output range.
     *
     * @param quality      quality values
     * @param parameters   trimming parameters
     * @param initialRange initial range to trim
     * @return trimmed range, or null in case the whole sequence should be trimmed
     */
    public static Range trim(SequenceQuality quality, QualityTrimmerParameters parameters, Range initialRange) {
        int lower = pabs(trim(quality, initialRange.getLower(), initialRange.getUpper(), +1, true, parameters)) + 1;
        assert lower >= initialRange.getLower();
        if (lower == initialRange.getUpper())
            return null;
        int upper = pabs(trim(quality, lower, initialRange.getUpper(), -1, true, parameters));
        if (upper == lower)
            // Should not happen, just in case
            return null;
        return new Range(lower, upper, initialRange.isReverse());
    }
}
