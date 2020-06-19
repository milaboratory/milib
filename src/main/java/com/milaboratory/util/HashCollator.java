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

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.primitivio.blocks.PrimitivIBlocks;
import com.milaboratory.primitivio.blocks.PrimitivOBlocks;
import com.milaboratory.primitivio.blocks.PrimitivOBlocksStats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HashCollator<T> {
    private static final int sizeRecheckPeriod = 1 << 15; // 32k

    private final Class<T> clazz;
    private final Comparator<T> comparator;
    private final ToIntFunction<T> hash;
    private final Path filePrefix;
    private final int bitsPerStep;
    private final long memoryBudget;
    private final int readerConcurrency, writerConcurrency;
    private final PrimitivOState oState;
    private final PrimitivIState iState;

    private final AtomicLongArray timeOnLevel = new AtomicLongArray(32);
    private final AtomicLong
            timeInCollate = new AtomicLong(),
            timeAwaitingO = new AtomicLong(),
            timeAwaitingI = new AtomicLong(),
            timeInFinalSorting1 = new AtomicLong(),
            timeInFinalSorting2 = new AtomicLong(),
            timeInFinalSorting3 = new AtomicLong();

    public HashCollator(Class<T> clazz,
                        ToIntFunction<T> hash, Comparator<T> comparator,
                        Path filePrefix, int bitsPerStep,
                        int readerConcurrency, int writerConcurrency) {
        this(clazz, hash, comparator, filePrefix, bitsPerStep, readerConcurrency, writerConcurrency,
                PrimitivOState.INITIAL, PrimitivIState.INITIAL);
    }

    public HashCollator(Class<T> clazz,
                        ToIntFunction<T> hash, Comparator<T> comparator,
                        Path filePrefix, int bitsPerStep,
                        int readerConcurrency, int writerConcurrency,
                        PrimitivOState oState, PrimitivIState iState) {
        this(clazz, hash, comparator, filePrefix, bitsPerStep, readerConcurrency, writerConcurrency,
                oState, iState, 1 << 28 /* 256 Mb */);
    }

    public HashCollator(Class<T> clazz,
                        ToIntFunction<T> hash, Comparator<T> comparator,
                        Path filePrefix, int bitsPerStep,
                        int readerConcurrency, int writerConcurrency,
                        PrimitivOState oState, PrimitivIState iState,
                        long memoryBudget) {
        this.clazz = clazz;
        this.hash = hash;
        this.comparator = comparator;
        if (Files.isDirectory(filePrefix))
            filePrefix = filePrefix.resolve("a");
        this.bitsPerStep = bitsPerStep;
        this.filePrefix = filePrefix;
        this.readerConcurrency = readerConcurrency;
        this.writerConcurrency = writerConcurrency;
        this.oState = oState;
        this.iState = iState;
        this.memoryBudget = memoryBudget;
    }

    public Comparator<T> effectiveComparator() {
        return new HComparator<>(hash, comparator);
    }

    public static <T> Comparator<T> effectiveComparator(ToIntFunction<T> hash, Comparator<T> comparator) {
        return new HComparator<>(hash, comparator);
    }

    public OutputPortCloseable<T> port(OutputPort<T> input) {
        Collater c = new Collater(input, filePrefix,
                bitsPerStep, 32 - bitsPerStep,
                128, true);
        c.run();
        return c.port();
    }

    public void printStat() {
        System.out.println("timeInCollate: " + FormatUtils.nanoTimeToString(timeInCollate.get()));
        System.out.println("timeAwaitingO: " + FormatUtils.nanoTimeToString(timeAwaitingO.get()));
        System.out.println("timeAwaitingI: " + FormatUtils.nanoTimeToString(timeAwaitingI.get()));
        System.out.println("timeInFinalSorting1: " + FormatUtils.nanoTimeToString(timeInFinalSorting1.get()));
        System.out.println("timeInFinalSorting2: " + FormatUtils.nanoTimeToString(timeInFinalSorting2.get()));
        System.out.println("timeInFinalSorting3: " + FormatUtils.nanoTimeToString(timeInFinalSorting3.get()));
    }

    private final class Collater implements Runnable {
        final OutputPort<T> source;
        final Path prefix;
        final int numberOfBuckets, bitCount, bitMask, bitOffset;
        final int[] bucketSizes;
        final boolean rootCollater;
        final AtomicBoolean initialized = new AtomicBoolean();
        long objectSize;

        public Collater(OutputPort<T> source, Path prefix,
                        int bitCount, int bitOffset,
                        long objectSizeInitialValue,
                        boolean rootCollater) {
            this.source = source;
            this.prefix = prefix;
            this.bitCount = bitCount;
            this.numberOfBuckets = 1 << bitCount;
            this.bitMask = ~(0xFFFFFFFF << bitCount);
            this.bitOffset = bitOffset;
            this.objectSize = objectSizeInitialValue;
            this.rootCollater = rootCollater;
            this.bucketSizes = new int[numberOfBuckets];
        }

        public Path getBucketPath(int i) {
            return prefix.resolveSibling(prefix.getFileName() + "." + i);
        }

        /**
         * Budget available for in-memory bucket buffering before submitting blocks to primitivOBlocks
         */
        public long availableMemoryBudget() {
            // From:
            // averageBlockSize * (numberOfBuckets + readerConcurrency + writerConcurrency) = totalMemoryBudget
            return memoryBudget * numberOfBuckets / (numberOfBuckets + readerConcurrency + writerConcurrency);
        }

        @Override
        public void run() {
            long runStart = System.nanoTime();
            try {
                PrimitivOBlocks<T> o = new PrimitivOBlocks<>(writerConcurrency, oState, 1);

                // Blocks by bucket
                ArrayList<T>[] blocks = new ArrayList[numberOfBuckets];
                // Bucket writers
                PrimitivOBlocks<T>.Writer[] os = new PrimitivOBlocks.Writer[numberOfBuckets];

                for (int i = 0; i < numberOfBuckets; i++) {
                    blocks[i] = new ArrayList<>();
                    os[i] = o.newWriter(getBucketPath(i));
                }

                T obj;
                int objectsCount = 0;
                long objectSize = this.objectSize;
                int recheckCounter = sizeRecheckPeriod;
                while ((obj = source.take()) != null) {
                    int bucketId = bitMask & (hash.applyAsInt(obj) >>> bitOffset);
                    blocks[bucketId].add(obj);
                    objectsCount++;

                    // Adjusting object size estimate based ob observed serialized size
                    // Dynamic adjustment performed only for root collater,
                    // nested collaters uses fixed value from the root
                    if (rootCollater && recheckCounter-- == 0) {
                        recheckCounter = sizeRecheckPeriod;
                        PrimitivOBlocksStats stats = o.getStats();
                        if (stats.objectCount > sizeRecheckPeriod)
                            objectSize = stats.getAverageUncompressedObjectSize();
                    }

                    if (objectsCount * objectSize >= availableMemoryBudget()) { // Memory budget over
                        // Finding biggest bucket
                        int maxBucketSize = 0, maxBucketId = 0;
                        for (int i = 0; i < numberOfBuckets; i++)
                            if (blocks[i].size() > maxBucketSize) {
                                maxBucketId = i;
                                maxBucketSize = blocks[i].size();
                            }

                        // Writing it to a corresponding file
                        long start = System.nanoTime();
                        os[maxBucketId].writeBlock(blocks[maxBucketId]);
                        timeAwaitingO.addAndGet(System.nanoTime() - start);
                        // Subtracting size of the block from the current object budget
                        objectsCount -= maxBucketSize;
                        // Creating new block for the bucket
                        blocks[maxBucketId] = new ArrayList<>();
                        // Counting number of objects in the bucket
                        this.bucketSizes[maxBucketId] += maxBucketSize;
                    }
                }

                // Writing final blocks
                for (int i = 0; i < numberOfBuckets; i++) {
                    if (!blocks[i].isEmpty()) {
                        long start = System.nanoTime();
                        os[i].writeBlock(blocks[i]);
                        timeAwaitingO.addAndGet(System.nanoTime() - start);
                    }
                    blocks[i] = null; // for GC
                }

                for (int i = 0; i < numberOfBuckets; i++)
                    os[i].close(); // Also perform sync

                // Initialization done
                initialized.set(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            timeInCollate.addAndGet(System.nanoTime() - runStart);
        }

        private OutputPort<T> getBucketRawPort(int i) {
            try {
                PrimitivIBlocks<T> input = new PrimitivIBlocks<>(clazz, readerConcurrency, iState);
                Path bucketPath = getBucketPath(i);
                PrimitivIBlocks<T>.Reader reader = input.newReader(bucketPath, readerConcurrency);
                return new OutputPortCloseable<T>() {
                    @Override
                    public T take() {
                        try {
                            long start = System.nanoTime();
                            T obj = reader.take();
                            timeAwaitingI.addAndGet(System.nanoTime() - start);
                            if (obj == null)
                                Files.delete(bucketPath);
                            return obj;
                        } catch (RuntimeException e) {
                            reader.close();
                            throw e;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void close() {
                        try {
                            reader.close();
                            Files.delete(bucketPath);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public OutputPortCloseable<T> getPortForBucket(int i) {
            if (!initialized.get())
                throw new IllegalStateException();

            if (bucketSizes[i] * objectSize > memoryBudget) {

                // <- requires additional HDD based collate procedure

                // Bits to fit each sub-bucket into budget
                int nextBitCount = 64 - Long.numberOfLeadingZeros(bucketSizes[i] * objectSize / memoryBudget - 1);
                nextBitCount += 1;
                nextBitCount = Math.min(nextBitCount, bitCount);

                int newOffset = bitOffset - nextBitCount;
                if (newOffset < 0)
                    throw new IllegalStateException("Can't fit into memory budget.");

                Collater c = new Collater(getBucketRawPort(i), getBucketPath(i),
                        nextBitCount, newOffset, objectSize, false);

                // Synchronous bucket separation
                c.run();

                return c.port();
            } else {
                // Reading bucket into memory (it is small enough to fit into memory budget)
                int fBitCount = Math.min(bitOffset, 15);
                int fNumberOfBuckets = 1 << fBitCount;
                int fOffset = bitOffset - fBitCount;
                int fBitMask = ~(0xFFFFFFFF << fBitCount);

                ArrayList<T>[] fBuckets = new ArrayList[fNumberOfBuckets];
                for (T t : CUtils.it(getBucketRawPort(i))) {
                    long start = System.nanoTime();
                    int bucket = fBitMask & (hash.applyAsInt(t) >>> fOffset);
                    ArrayList<T> fBucket = fBuckets[bucket];
                    if (fBucket == null)
                        fBuckets[bucket] = fBucket = new ArrayList<>();
                    fBucket.add(t);
                    timeInFinalSorting1.addAndGet(System.nanoTime() - start);
                }

                long start = System.nanoTime();
                Arrays.stream(fBuckets).parallel().forEach(
                        fBucket -> {
                            if (fBucket != null)
                                if (fOffset == 0)
                                    fBucket.sort(comparator);
                                else
                                    fBucket.sort(effectiveComparator());
                        }
                );
                timeInFinalSorting2.addAndGet(System.nanoTime() - start);

                start = System.nanoTime();
                List<T> list = Arrays.stream(fBuckets)
                        .flatMap(d -> d == null ? Stream.empty() : d.stream())
                        .collect(Collectors.toList());
                timeInFinalSorting3.addAndGet(System.nanoTime() - start);

                // Returning in-memory stream
                OutputPort<T> op = CUtils.asOutputPort(list);
                return new OutputPortCloseable<T>() {
                    @Override
                    public void close() {
                        // noop
                    }

                    @Override
                    public T take() {
                        return op.take();
                    }
                };
            }
        }

        public OutputPortCloseable<T> port() {
            return new OutputPortCloseable<T>() {
                int nextBucket = 1;
                OutputPortCloseable<T> currentPort = getPortForBucket(0);
                OutputPortCloseable<T> nextPort = getPortForBucket(1);

                @Override
                public synchronized T take() {
                    T obj;
                    while ((obj = currentPort.take()) == null && nextBucket < numberOfBuckets) {
                        currentPort = nextPort;
                        nextBucket++;
                        if (nextBucket < numberOfBuckets)
                            nextPort = getPortForBucket(nextBucket);
                    }
                    return obj;
                }

                @Override
                public synchronized void close() {
                    currentPort.close();
                    nextBucket = numberOfBuckets;
                }
            };
        }
    }

    private static final class HComparator<T> implements Comparator<T> {
        final ToIntFunction<T> hash;
        final Comparator<T> comparator;

        public HComparator(ToIntFunction<T> hash, Comparator<T> comparator) {
            this.hash = hash;
            this.comparator = comparator;
        }

        @Override
        public int compare(T o1, T o2) {
            int c = Integer.compareUnsigned(hash.applyAsInt(o1), hash.applyAsInt(o2));
            if (c != 0)
                return c;
            return comparator.compare(o1, o2);
        }
    }
}
