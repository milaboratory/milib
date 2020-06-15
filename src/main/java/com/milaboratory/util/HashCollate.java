/*
 * Copyright 2020 MiLaboratory, LLC
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
package com.milaboratory.util;

import cc.redberry.pipe.OutputPort;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.primitivio.blocks.PrimitivOBlocks;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;

public class HashCollate<T> {
    private final long targetBlockSize = 1 << 20; // 1 Mb
    private final int readerConcurrency, writerConcurrency;
    private final PrimitivOState oState;
    private final PrimitivIState iState;

    public HashCollate(int readerConcurrency, int writerConcurrency,
                       PrimitivOState oState, PrimitivIState iState) {
        this.readerConcurrency = readerConcurrency;
        this.writerConcurrency = writerConcurrency;
        this.oState = oState;
        this.iState = iState;
    }

    private final class Collater implements Runnable {
        final OutputPort<T> source;
        final Path prefix;
        final int numberOfBuckets, bitMask, bitOffset;
        final long objectSizeInitialGuess;

        public Collater(OutputPort<T> source, Path prefix,
                        int bitCount, int bitOffset,
                        long objectSizeInitialGuess) {
            this.source = source;
            this.prefix = prefix;
            this.numberOfBuckets = 1 << bitCount;
            this.bitMask = ~(0xFFFFFFFF << bitCount);
            this.bitOffset = bitOffset;
            this.objectSizeInitialGuess = objectSizeInitialGuess;
        }

        public Path bucketPath(int i) {
            return prefix.resolveSibling(prefix.getFileName() + "." + i);
        }

        @Override
        public void run() {
            try {
                PrimitivOBlocks<T> o = new PrimitivOBlocks<>(writerConcurrency, oState, 1);

                // Blocks by bucket
                ArrayList<T>[] blocks = new ArrayList[numberOfBuckets];
                // Bucket writers
                PrimitivOBlocks<T>.Writer[] os = new PrimitivOBlocks.Writer[numberOfBuckets];

                for (int i = 0; i < numberOfBuckets; i++) {
                    blocks[i] = new ArrayList<>();

                    os[i] = o.newWriter(bucketPath(i));

                }

                T obj;
                while ((obj = source.take()) != null) {
                    int bucket = obj.hashCode();

                    blocks[]
                }
            } catch (IOException e) {
                // TODO ???
                e.printStackTrace();
            }
        }
    }

    private static final class HComparator<T> implements Comparator<T> {
        final Comparator<T> comparator;

        public HComparator(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(T o1, T o2) {
            int c = Integer.compareUnsigned(o1.hashCode(), o2.hashCode());
            if (c != 0)
                return c;
            return comparator.compare(o1, o2);
        }
    }
}
