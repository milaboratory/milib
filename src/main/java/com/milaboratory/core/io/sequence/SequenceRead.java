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

import com.milaboratory.core.sequence.quality.FunctionWithIndex;
import com.milaboratory.primitivio.annotations.Serializable;

import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
@Serializable(by = IO.SequenceReadSerializer.class)
public interface SequenceRead extends Iterable<SingleRead> {
    int numberOfReads();

    SingleRead getRead(int i);

    long getId();

    default SequenceRead mapReads(Function<SingleRead, SingleRead> mapping) {
        return SequenceReadUtil.construct(IntStream.range(0, numberOfReads())
                .mapToObj(this::getRead)
                .map(mapping)
                .toArray(SingleRead[]::new));
    }

    default SequenceRead mapReadsWithIndex(FunctionWithIndex<SingleRead, SingleRead> mapping) {
        return SequenceReadUtil.construct(
                IntStream.range(0, numberOfReads())
                        .mapToObj(i -> mapping.apply(i, this.getRead(i)))
                        .toArray(SingleRead[]::new));
    }
}
