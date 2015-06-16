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

import com.milaboratory.core.io.sequence.fastq.QualityFormat;
import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.UnsafeFactory;
import com.milaboratory.util.SingleIterator;

import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public abstract class SingleReadLazy implements SingleRead {
    final long id;
    final byte[] buffer;
    final int descriptionFrom;
    final short sequenceOffset, qualityOffset, dataLength, descriptionLength;
    NSequenceWithQuality sequenceWithQuality;
    String description;

    private SingleReadLazy(long id,
                           byte[] buffer,
                           int descriptionFrom,
                           short sequenceOffset,
                           short qualityOffset,
                           short dataLength,
                           short descriptionLength) {
        this.id = id;
        this.buffer = buffer;
        this.descriptionFrom = descriptionFrom;
        this.sequenceOffset = sequenceOffset;
        this.qualityOffset = qualityOffset;
        this.dataLength = dataLength;
        this.descriptionLength = descriptionLength;
    }

    abstract byte getQualityOffset();

    @Override
    public String getDescription() {
        if (description == null)
            description = createDescription();
        return description;
    }

    @Override
    public NSequenceWithQuality getData() {
        if (sequenceWithQuality == null)
            sequenceWithQuality = createNSequenceWithQuality();
        return sequenceWithQuality;
    }

    private String createDescription() {
        return new String(buffer, descriptionFrom, descriptionLength, Charset.defaultCharset());
    }

    private NSequenceWithQuality createNSequenceWithQuality() {
        return UnsafeFactory.fastqParse(buffer, descriptionFrom + sequenceOffset,
                descriptionFrom + qualityOffset, dataLength, getQualityOffset(), id);
    }

    @Override
    public int numberOfReads() {
        return 1;
    }

    @Override
    public SingleRead getRead(int i) {
        if (i != 0)
            throw new IndexOutOfBoundsException();
        return this;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Iterator<SingleRead> iterator() {
        return new SingleIterator<>((SingleRead) this);
    }

    public static SingleReadLazy create(final QualityFormat format,
                                        long id,
                                        byte[] buffer,
                                        int descriptionFrom,
                                        short dataOffset,
                                        short qualityOffset,
                                        short dataLength,
                                        short descriptionLength) {
        if (format == QualityFormat.Phred33)
            return new SingleReadLazy(id, buffer, descriptionFrom, dataOffset, qualityOffset, dataLength, descriptionLength) {
                @Override
                byte getQualityOffset() {
                    return 33;
                }
            };
        else if (format == QualityFormat.Phred64)
            return new SingleReadLazy(id, buffer, descriptionFrom, dataOffset, qualityOffset, dataLength, descriptionLength) {
                @Override
                byte getQualityOffset() {
                    return 64;
                }
            };
        throw new IllegalArgumentException("Unknown quality format.");
    }
}
