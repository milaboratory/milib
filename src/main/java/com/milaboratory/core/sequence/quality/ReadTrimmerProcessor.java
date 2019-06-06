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

import cc.redberry.pipe.Processor;
import cc.redberry.primitives.Filter;
import com.milaboratory.core.Range;
import com.milaboratory.core.io.sequence.SequenceRead;
import com.milaboratory.core.sequence.NSequenceWithQuality;

public final class ReadTrimmerProcessor<R extends SequenceRead> implements Processor<R, R> {
    public static final Filter<SequenceRead> NON_EMPTY_READ_FILTER = object -> {
        for (int i = 0; i < object.numberOfReads(); i++) {
            if (object.getRead(i).getData().size() != 0)
                return true;
        }
        return false;
    };

    final QualityTrimmerParameters trimmingParameters;
    final ReadTrimmerListener listener;

    public ReadTrimmerProcessor(QualityTrimmerParameters trimmingParameters) {
        this(trimmingParameters, null);
    }

    public ReadTrimmerProcessor(QualityTrimmerParameters trimmingParameters, ReadTrimmerListener listener) {
        this.trimmingParameters = trimmingParameters;
        this.listener = listener;
    }

    @Override
    public R process(R input) {
        //noinspection unchecked
        return (R) input.mapReadsWithIndex((i, singleRead) ->
                singleRead.mapSequence(nsq -> {
                    Range range = QualityTrimmer.bestIsland(nsq.getQuality(), trimmingParameters);
                    if (listener != null)
                        listener.onSequence(input, i, range,
                                range == null
                                        || range.getLower() != 0
                                        || range.getUpper() != nsq.size());
                    if (range == null)
                        return NSequenceWithQuality.EMPTY;
                    if (range.getLower() == 0 && range.getUpper() == nsq.size())
                        return nsq;
                    return nsq.getRange(range);
                })
        );
    }
}
