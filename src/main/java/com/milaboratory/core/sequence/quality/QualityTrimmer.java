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

public class QualityTrimmer {
    /**
     * Core trimming method. Implements main algorithm that finds optimal trimming position.
     *
     * @param quality               sequence quality
     * @param startPosition         position to start trimming from
     * @param scanLeft              true to scan left (trim on the right side of the sequence); false to trim right
     * @param minimalTestRegion     minimal number of positions to scan
     * @param minimalAverageQuality target minimal average quality
     * @return trimming position; e.g. startPosition will be return if trimming is not required
     */
    public static int trim(SequenceQuality quality,
                           int startPosition, int scanLeft,
                           int minimalTestRegion,
                           float minimalAverageQuality) {
        int maxIterations = scanLeft ? startPosition
        minimalTestRegion
    }
}
