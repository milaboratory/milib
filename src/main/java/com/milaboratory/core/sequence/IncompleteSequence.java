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
package com.milaboratory.core.sequence;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
abstract class IncompleteSequence<IS extends IncompleteSequence<IS, S>, S extends Sequence<S>>
        extends AbstractArraySequence<IS> {
    IncompleteSequence(byte[] data) {
        super(data);
    }

    protected IncompleteSequence(String sequence) {
        super(sequence);
    }

    @Override
    public abstract IncompleteAlphabet<IS, S> getAlphabet();

    public final boolean isComplete() {
        for (byte c : data)
            if (c == getAlphabet().getUnknownLetterCode())
                return false;
        return true;
    }

    public final S convertToComplete() {
        if (!isComplete())
            throw new RuntimeException("This sequence contains incomplete parts.");
        SequenceBuilder<S> builder = getAlphabet().getOrigin()
                .getBuilder().ensureCapacity(data.length);
        for (byte b : data)
            builder.append(b);
        return builder.createAndDestroy();
    }
}
