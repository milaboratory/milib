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
package com.milaboratory.core.io.sequence;

import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.quality.FunctionWithIndex;

import java.util.function.Function;

/**
 * Single read
 *
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public interface SingleRead extends SequenceRead, java.io.Serializable {
    String getDescription();

    NSequenceWithQuality getData();

    @Override
    default SingleRead mapReads(Function<SingleRead, SingleRead> mapping) {
        return mapping.apply(this);
    }

    default SingleRead mapSequence(Function<NSequenceWithQuality, NSequenceWithQuality> mapping) {
        final NSequenceWithQuality mapped = mapping.apply(this.getData());
        if (mapped == getData())
            return this;
        return new SingleReadImpl(this.getId(), mapped, this.getDescription());
    }

    @Override
    default SingleRead mapReadsWithIndex(FunctionWithIndex<SingleRead, SingleRead> mapping) {
        return mapping.apply(0, this);
    }
}
