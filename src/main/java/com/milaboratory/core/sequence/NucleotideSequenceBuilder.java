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

import com.milaboratory.util.Bit2Array;

/**
 * {@code SequenceBuilder} for nucleotide sequences.
 *
 * @see com.milaboratory.core.sequence.NucleotideSequence
 * @see com.milaboratory.core.sequence.SequenceBuilder
 */
public final class NucleotideSequenceBuilder implements SequenceBuilder<NucleotideSequence> {
    private Bit2Array storage;
    private int size = 0;

    /**
     * Creates {@code SequenceBuilder} with empty initial buffer.
     */
    public NucleotideSequenceBuilder() {
        this.storage = null;
    }

    private NucleotideSequenceBuilder(Bit2Array storage, int size) {
        this.storage = storage;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public NucleotideSequenceBuilder ensureCapacity(int capacity) {
        if (size == -1)
            throw new IllegalStateException("Destroyed.");
        if (capacity > 0) {
            if (storage == null)
                storage = new Bit2Array(capacity);
            else if (capacity > storage.size())
                storage = storage.extend(capacity);
        }
        return this;
    }

    private void ensureInternalCapacity(int newSize) {
        if (size == -1)
            throw new IllegalStateException("Destroyed.");
        if (newSize > 0) {
            if (storage == null)
                storage = new Bit2Array(Math.max(newSize, 10));
            if (storage.size() < newSize)
                storage = storage.extend(Math.max(newSize, 3 * storage.size() / 2 + 1));
        }
    }

    @Override
    public NucleotideSequence createAndDestroy() {
        if (size == 0)
            return NucleotideSequence.EMPTY;
        NucleotideSequence seq;
        if (storage.size() == size)
            seq = new NucleotideSequence(storage, true);
        else
            seq = new NucleotideSequence(storage.getRange(0, size), true);
        storage = null;
        size = -1;
        return seq;
    }

    @Override
    public NucleotideSequenceBuilder append(byte[] letters) {
        ensureInternalCapacity(size + letters.length);
        int size = this.size;
        for (byte letter : letters)
            storage.set(size++, letter);
        this.size = size;
        return this;
    }

    @Override
    public NucleotideSequenceBuilder set(int position, byte letter) {
        if (position < 0 || position >= size)
            throw new IndexOutOfBoundsException();
        storage.set(position, letter);
        return this;
    }

    @Override
    public NucleotideSequenceBuilder append(byte letter) {
        ensureInternalCapacity(size + 1);
        storage.set(size++, letter);
        return this;
    }

    @Override
    public NucleotideSequenceBuilder append(NucleotideSequence seq) {
        if (seq.size() == 0)
            return this;
        ensureInternalCapacity(size + seq.size());
        storage.copyFrom(seq.data, 0, size, seq.size());
        size += seq.size();
        return this;
    }

    @Override
    public NucleotideSequenceBuilder clone() {
        return new NucleotideSequenceBuilder(storage == null ? null : storage.clone(), size);
    }
}
