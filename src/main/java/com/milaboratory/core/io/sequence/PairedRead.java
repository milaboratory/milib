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

import java.util.function.Function;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class PairedRead extends MultiRead {
    public PairedRead(SingleRead... data) {
        super(data);
        if (data.length != 2)
            throw new IllegalArgumentException();
    }

    public SingleRead getR1() {
        return data[0];
    }

    public SingleRead getR2() {
        return data[1];
    }

    @Override
    public PairedRead mapReads(Function<SingleRead, SingleRead> mapping) {
        return new PairedRead(
                mapping.apply(data[0]),
                mapping.apply(data[1])
        );
    }

    @Override
    public PairedRead mapReadsWithIndex(FunctionWithIndex<SingleRead, SingleRead> mapping) {
        return new PairedRead(
                mapping.apply(0, data[0]),
                mapping.apply(1, data[1])
        );
    }

    @Override
    public String toString() {
        return getR1().toString() + "\n" + getR2().toString();
    }
}
