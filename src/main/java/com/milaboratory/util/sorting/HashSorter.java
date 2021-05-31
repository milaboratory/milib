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
package com.milaboratory.util.sorting;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.primitivio.blocks.PrimitivIBlocks;
import com.milaboratory.primitivio.blocks.PrimitivIOBlocksUtil;
import com.milaboratory.primitivio.blocks.PrimitivOBlocks;
import com.milaboratory.primitivio.blocks.PrimitivOBlocksStats;
import com.milaboratory.util.FormatUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO implement minimal budget in-memory analysis (don't use hdd if whole analysis can be done in memory)

/**
 * Implements HDD-offloading sorter, that sorts objects by a defined hash code (more specifically it's unsigned value)
 * first, and by a defined comparator if objects has the same hash code.
 *
 * @param <T> type of objects to sort
 */
public class HashSorter<T> {
    private static final int sizeRecheckPeriod = 1 << 15; // 32k objects

    private static final long maxBlockSize = 1 << 23; // 8 Mb (more or less optimal for LZ4 compressor)

    /** Object class, used in deserialization. */
    private final Class<T> clazz;

    /** Target hash function */
    private final ToIntFunction<T> hash;
    /** Target comparator */
    private final Comparator<T> comparator;

    /** Has code bits per collation step */
    private final int bitsPerStep;
    /** Maximal memory usage */
    private final long memoryBudget;

    /** Object size initial guess */
    private final long objectSizeInitialGuess;


    /** Path prefix for temporary files */
    private final Path filePrefix;

    /** Maximal concurrency for IO operations */
    private final int readerConcurrency, writerConcurrency;

    /** OState for serialization */
    private final PrimitivOState oState;
    /** IState for deserialization */
    private final PrimitivIState iState;

    /** This class effectively sorts objects according to this comparator */
    private final Comparator<T> effectiveComparator;

    // Stats

    private final CopyOnWriteArrayList<CollationNodeInfo> nodeInfos = new CopyOnWriteArrayList<>();

    private final AtomicLongArray timeOnLevel = new AtomicLongArray(32);
    private final AtomicLong
            timeInCollate = new AtomicLong(),
            timeInCollatorInit = new AtomicLong(),
            timeAwaitingO = new AtomicLong(),
            timeAwaitingI = new AtomicLong(),
            timeInFinalSorting1 = new AtomicLong(),
            timeInFinalSorting2 = new AtomicLong(),
            timeInFinalSorting3 = new AtomicLong(),
            totalFilesUsed = new AtomicLong(),
            maxDepth = new AtomicLong();

    /**
     * Creates hash sorter. Actual sorting starts on {@link #port(OutputPort)} invocation.
     *
     * @param clazz                  object class
     * @param hash                   target hash function
     * @param comparator             target comparator for objects with equal hash codes
     * @param bitsPerStep            number of bits to use n each collation step;
     *                               number of buckets on each step = 2 ^ bitsPerStep
     * @param filePrefix             path prefix for temporary files
     * @param readerConcurrency      read / deserialization concurrency
     * @param writerConcurrency      write / serialization concurrency
     * @param oState                 oState for serialization
     * @param iState                 iState for deserialization
     * @param memoryBudget           maximal allowed memory consumption
     * @param objectSizeInitialGuess initial guess for single object size;
     *                               underestimation of this value may lead to temporary overconsumption of memory
     */
    public HashSorter(Class<T> clazz,
                      ToIntFunction<T> hash, Comparator<T> comparator,
                      int bitsPerStep, Path filePrefix,
                      int readerConcurrency, int writerConcurrency,
                      PrimitivOState oState, PrimitivIState iState,
                      long memoryBudget, long objectSizeInitialGuess) {
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
        this.objectSizeInitialGuess = objectSizeInitialGuess;
        this.effectiveComparator = getEffectiveComparator(hash, comparator);
    }

    public Comparator<T> getEffectiveComparator() {
        return effectiveComparator;
    }

    public static <T> Comparator<T> getEffectiveComparator(ToIntFunction<T> hash, Comparator<T> comparator) {
        return (o1, o2) -> compare(hash, comparator, o1, o2);
    }

    public OutputPortCloseable<T> port(OutputPort<T> input) {
        Collator c = new Collator(CollatorAddress.ROOT,
                input, filePrefix,
                bitsPerStep, 32 - bitsPerStep,
                objectSizeInitialGuess, new Object[0]);
        c.run();
        return c.port();
    }

    /**
     * After successful collation this method returns number of hash-sorting nodes (external files) utilized in the process.
     */
    public int getNumberOfNodes() {
        return nodeInfos.size();
    }

    public void printStat() {
        printStat(true);
    }

    public void printStat(boolean withNodeStat) {
        System.out.println("timeInCollate: " + FormatUtils.nanoTimeToString(timeInCollate.get()));
        System.out.println("timeInCollatorInit: " + FormatUtils.nanoTimeToString(timeInCollatorInit.get()));
        System.out.println("timeAwaitingO: " + FormatUtils.nanoTimeToString(timeAwaitingO.get()));
        System.out.println("timeAwaitingI: " + FormatUtils.nanoTimeToString(timeAwaitingI.get()));
        System.out.println("timeInFinalSorting1: " + FormatUtils.nanoTimeToString(timeInFinalSorting1.get()));
        System.out.println("timeInFinalSorting2: " + FormatUtils.nanoTimeToString(timeInFinalSorting2.get()));
        System.out.println("timeInFinalSorting3: " + FormatUtils.nanoTimeToString(timeInFinalSorting3.get()));
        if (withNodeStat)
            for (CollationNodeInfo nodeInfo : nodeInfos)
                System.out.println(nodeInfo);
    }

    private final class CollatorStatAggregator {
        final int maxSingletonBuckets;
        final double samplingPeriodMultiplier;
        double samplingPeriod;

        final SortedMap<T, Long> objectStats = new TreeMap<>(getEffectiveComparator());

        long sampledObjectCount = 0, sampledNonSingletonObjectCount = 0, skippedObjects = 0;
        double samplingCounter = 0;

        public CollatorStatAggregator(int maxSingletonBuckets, double samplingPeriod, double samplingPeriodMultiplier) {
            this.maxSingletonBuckets = maxSingletonBuckets;
            this.samplingPeriod = samplingPeriod;
            this.samplingPeriodMultiplier = samplingPeriodMultiplier;
        }

        long getSampledObjectCount() {
            return sampledObjectCount;
        }

        long getSampledNonSingletonObjectCount() {
            return sampledNonSingletonObjectCount;
        }

        Object[] getSingletonObjects() {
            return objectStats.keySet().toArray(new Object[0]);
        }

        boolean isSingleton() {
            return skippedObjects == 0 && objectStats.size() == 1;
        }

        void putObject(T object) {
            // Activating down-sampling after at least two different objects are found
            if (objectStats.size() > 1) {
                samplingCounter += 1;
                if (samplingCounter >= 0)
                    samplingCounter -= (samplingPeriod *= samplingPeriodMultiplier);
                else {
                    ++skippedObjects;
                    return;
                }
            }

            ++sampledObjectCount;

            if (maxSingletonBuckets == 0) {
                ++skippedObjects;
                return;
            }

            objectStats.compute(object, (__, n) -> n == null ? 1 : n + 1);
            if (objectStats.size() > maxSingletonBuckets) {
                // Removing element with the smallest count (value in map)
                Map.Entry<T, Long> minEntry = objectStats.entrySet().stream()
                        .min(Map.Entry.comparingByValue())
                        .get();
                objectStats.remove(minEntry.getKey());
                sampledNonSingletonObjectCount += minEntry.getValue();
            }
        }
    }

    /**
     * Maps object to a particular bucket.
     * <p>
     * Bucket structure with a single singleton object:
     * <pre>
     * |  -- Hash Bucket 0 --  |  -- Hash Bucket 1 --  | ... |  -- Hash Bucket 2 ^ bitCount - 1 -- |
     * |          B0(N)        | B1(N) | B2(S) | B3(N) | ... |         B[2 ^ bitCount + 1](N)      |
     *                                     ^ Singleton
     * </pre>
     */
    final static class BucketMapping<T> {
        private final ToIntFunction<T> hash;
        private final Comparator<T> effectiveComparator;

        final int numberOfHashBuckets, bitCount, bitMask, bitOffset, numberOfBuckets;

        /**
         * hash bucket id -> index of the first singleton in this hash bucket
         * first element always equals zero
         * last element with index numberOfHashBuckets always equals to number of singletons
         */
        final int[] singletonPointers;
        /**
         * firstSubBucketId[i] - stores first sub-bucket id inside the i-th hash bucket
         */
        final int[] firstSubBucketId;

        final Object[] singletons;

        @SuppressWarnings("unchecked")
        public BucketMapping(ToIntFunction<T> hash, Comparator<T> comparator,
                             int bitCount, int bitOffset, Object[] singletons) {
            this.hash = hash;
            this.effectiveComparator = getEffectiveComparator(hash, comparator);
            this.singletons = singletons;

            this.bitCount = bitCount;
            this.numberOfHashBuckets = 1 << bitCount;
            this.bitMask = ~(0xFFFFFFFF << bitCount);
            this.bitOffset = bitOffset;

            this.singletonPointers = new int[numberOfHashBuckets + 1];
            this.firstSubBucketId = new int[numberOfHashBuckets];

            int hb = 0; // initialization to make assert line compilable
            for (int i = 0, j = 0; i < numberOfHashBuckets; i++) {
                firstSubBucketId[i] = i + singletonPointers[i] * 2;
                while (singletons.length != j && (hb = getHashBucketId((T) singletons[j])) == i)
                    ++j;
                singletonPointers[i + 1] = j;
                assert singletons.length == j || hb > i; // check that singletons array is properly sorted
            }
            this.numberOfBuckets = numberOfHashBuckets + singletons.length * 2;
        }

        public int getHashBucketId(T obj) {
            return bitMask & (hash.applyAsInt(obj) >>> bitOffset);
        }

        public boolean isSingletonBucket(int id) {
            int hb = Arrays.binarySearch(firstSubBucketId, id);
            if (hb >= 0) // (e.g. B1 or B2 from the scheme above)
                return false;
            hb = -hb - 2;
            assert hb >= 0;
            return (id - firstSubBucketId[hb]) % 2 == 1; // singleton buckets have odd local indices inside hash buckets
        }

        public int getNumberOfBuckets() {
            return numberOfBuckets;
        }

        public int getBucketId(T obj) {
            int hb = getHashBucketId(obj);

            @SuppressWarnings("unchecked")
            int singletonId = Arrays.binarySearch(singletons, singletonPointers[hb], singletonPointers[hb + 1],
                    obj, (Comparator<Object>) effectiveComparator);

            if (singletonId >= 0)
                return 1 + singletonId * 2 + hb;
            else
                return (-1 - singletonId) * 2 + hb;
        }
    }


    private final class Collator implements Runnable {
        final CollatorAddress address;
        final OutputPort<T> source;
        final Path prefix;

        final BucketMapping mapping;
        final int[] bucketObjectCounts;

        // Collator initializers are allocated only for hash-based buckets
        final CollatorStatAggregator[] collatorInitializers;

        // Singleton buckets have indices starting from numberOfHashBuckets
        final Object[] singletons;

        final AtomicBoolean initialized = new AtomicBoolean();
        long objectSize;

        public Collator(CollatorAddress address,
                        OutputPort<T> source, Path prefix,
                        int bitCount, int bitOffset,
                        long objectSizeInitialValue,
                        Object[] singletons) {
            this.address = address;
            this.source = source;
            this.prefix = prefix;

            this.mapping = new BucketMapping(hash, comparator, bitCount, bitOffset, singletons);

            this.objectSize = objectSizeInitialValue;
            this.bucketObjectCounts = new int[mapping.getNumberOfBuckets()];

            this.singletons = singletons;

            this.collatorInitializers = new HashSorter.CollatorStatAggregator[mapping.getNumberOfBuckets()];
            for (int i = 0; i < mapping.getNumberOfBuckets(); i++)
                if (!mapping.isSingletonBucket(i))
                    this.collatorInitializers[i] = new CollatorStatAggregator(1 << Math.max(1, bitsPerStep - 1), 1.0, 1.15); // TODO requires some empiric for parameter values
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
            return memoryBudget * mapping.getNumberOfBuckets() / (mapping.getNumberOfBuckets() + readerConcurrency + writerConcurrency);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            long runStart = System.nanoTime();

            try {
                PrimitivOBlocks<T> o = new PrimitivOBlocks<>(writerConcurrency, oState, 1, // block size not used
                        PrimitivIOBlocksUtil.fastLZ4Compressor());

                // Blocks by bucket
                ArrayList<T>[] blocks = new ArrayList[mapping.getNumberOfBuckets()];
                // Bucket writers
                PrimitivOBlocks<T>.Writer[] os = new PrimitivOBlocks.Writer[mapping.getNumberOfBuckets()];

                for (int i = 0; i < mapping.getNumberOfBuckets(); i++)
                    blocks[i] = new ArrayList<>();

                T obj;
                int objectsCount = 0;
                long objectSize = this.objectSize;
                int recheckCounter = sizeRecheckPeriod;
                int maxBucketSize = 0, maxBucketId = 0;
                while ((obj = source.take()) != null) {
                    // Adjusting object size estimate based on observed serialized size
                    // Dynamic adjustment performed only for root collator,
                    // nested collators uses fixed value from the root
                    if (address.isRoot() && recheckCounter-- == 0) {
                        recheckCounter = sizeRecheckPeriod;
                        PrimitivOBlocksStats stats = o.getStats();
                        if (stats.objectCount > sizeRecheckPeriod)
                            objectSize = stats.getAverageUncompressedObjectSize();
                    }

                    int bucketId = mapping.getBucketId(obj);
                    blocks[bucketId].add(obj);

                    if (blocks[bucketId].size() > maxBucketSize) {
                        maxBucketId = bucketId;
                        maxBucketSize = blocks[bucketId].size();
                    }

                    if (collatorInitializers[bucketId] != null) {
                        long start = System.nanoTime();
                        collatorInitializers[bucketId].putObject(obj);
                        timeInCollatorInit.addAndGet(System.nanoTime() - start);
                    }
                    objectsCount++;

                    if (objectsCount * objectSize >= availableMemoryBudget() ||
                            maxBucketSize * objectSize >= maxBlockSize) { // Memory budget overflow, or max block size reached
                        // Lazy writer initialization
                        if (os[maxBucketId] == null)
                            os[maxBucketId] = o.newWriter(getBucketPath(maxBucketId));

                        // Writing it to a corresponding file
                        long start = System.nanoTime();
                        os[maxBucketId].writeBlock(blocks[maxBucketId]);
                        timeAwaitingO.addAndGet(System.nanoTime() - start);
                        // Subtracting size of the block from the current object budget
                        objectsCount -= maxBucketSize;
                        // Creating new block for the bucket
                        blocks[maxBucketId] = new ArrayList<>();
                        // Counting number of objects in the bucket
                        bucketObjectCounts[maxBucketId] += maxBucketSize;

                        // Refreshing biggest bucket index
                        maxBucketSize = 0;
                        maxBucketId = 0;
                        for (int i = 0; i < mapping.getNumberOfBuckets(); i++)
                            if (blocks[i].size() > maxBucketSize) {
                                maxBucketId = i;
                                maxBucketSize = blocks[i].size();
                            }
                    }
                }

                // Writing final blocks
                for (int i = 0; i < mapping.getNumberOfBuckets(); i++) {
                    if (!blocks[i].isEmpty()) {
                        // Lazy writer initialization
                        if (os[i] == null)
                            os[i] = o.newWriter(getBucketPath(i));
                        long start = System.nanoTime();
                        os[i].writeBlock(blocks[i]);
                        bucketObjectCounts[i] += blocks[i].size();
                        timeAwaitingO.addAndGet(System.nanoTime() - start);
                    }
                    blocks[i] = null; // for GC
                }

                for (int i = 0; i < mapping.getNumberOfBuckets(); i++) {

                    if (os[i] == null)
                        continue;

                    os[i].close(); // Also performs sync

                    // Saving stats for this bucket
                    nodeInfos.add(
                            new CollationNodeInfo(address.resolve(i),
                                    mapping.numberOfBuckets, mapping.bitCount, mapping.bitOffset,
                                    bucketObjectCounts[i],
                                    os[i].getPosition(),
                                    mapping.isSingletonBucket(i) || collatorInitializers[i].isSingleton()));
                }

                // Initialization done
                initialized.set(true);

                // Saving actual object size
                this.objectSize = objectSize;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            timeInCollate.addAndGet(System.nanoTime() - runStart);
        }

        private OutputPortCloseable<T> getBucketRawPort(int i) {
            try {
                if (bucketObjectCounts[i] == 0)
                    return new OutputPortCloseable<T>() {
                        @Override
                        public void close() {
                        }

                        @Override
                        public T take() {
                            return null;
                        }
                    };

                PrimitivIBlocks<T> input = new PrimitivIBlocks<>(clazz, readerConcurrency, iState);
                Path bucketPath = getBucketPath(i);
                PrimitivIBlocks<T>.Reader reader = input.newReader(bucketPath, readerConcurrency);
                return new OutputPortCloseable<T>() {
                    final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public T take() {
                        T obj;

                        try {
                            long start = System.nanoTime();
                            obj = reader.take();
                            timeAwaitingI.addAndGet(System.nanoTime() - start);
                        } catch (RuntimeException e) {
                            close();
                            throw e;
                        }

                        if (obj == null)
                            close();

                        return obj;
                    }

                    @Override
                    public void close() {
                        if (!closed.compareAndSet(false, true))
                            return;
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

            if (mapping.isSingletonBucket(i) || collatorInitializers[i].isSingleton())
                return getBucketRawPort(i);

            if (bucketObjectCounts[i] * objectSize > memoryBudget) {

                // <- requires additional HDD based collate procedure

                // Bits to fit each sub-bucket into budget
                int nextBitCount = Math.min(
                        mapping.bitCount,
                        minimalNumberOfBits(bucketObjectCounts[i] * objectSize, memoryBudget)
                );

                int newOffset = mapping.bitOffset - nextBitCount;
                if (newOffset < 0)
                    throw new IllegalStateException("Can't fit into memory budget.");

                Collator c = new Collator(address.resolve(i),
                        getBucketRawPort(i), getBucketPath(i),
                        nextBitCount, newOffset, objectSize,
                        collatorInitializers[i].getSingletonObjects());

                // Synchronous bucket separation
                c.run();

                return c.port();
            } else {

                // <- reading bucket into memory (it is small enough to fit into memory budget)

                int fBitCount = Math.min(mapping.bitOffset, 15);
                int fNumberOfBuckets = 1 << fBitCount;
                int fOffset = mapping.bitOffset - fBitCount;
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
                                    fBucket.sort(getEffectiveComparator());
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
                    while ((obj = currentPort.take()) == null && nextBucket < mapping.getNumberOfBuckets()) {
                        currentPort = nextPort;
                        nextBucket++;
                        if (nextBucket < mapping.getNumberOfBuckets())
                            nextPort = getPortForBucket(nextBucket);
                    }
                    return obj;
                }

                @Override
                public synchronized void close() {
                    currentPort.close();
                    nextBucket = mapping.getNumberOfBuckets();
                }
            };
        }
    }

    public static <T> int compare(ToIntFunction<T> hash, Comparator<T> comparator, T o1, T o2) {
        int c = Integer.compareUnsigned(hash.applyAsInt(o1), hash.applyAsInt(o2));
        if (c != 0)
            return c;
        return comparator.compare(o1, o2);
    }

    public static final class CollatorAddress {
        private static final CollatorAddress ROOT = new CollatorAddress(new int[0]);
        private final int[] bucketIndices;

        private CollatorAddress(int[] bucketIndices) {
            this.bucketIndices = bucketIndices;
        }

        public int getDepth() {
            return bucketIndices.length;
        }

        public boolean isRoot() {
            return getDepth() == 0;
        }

        public CollatorAddress resolve(int bucketIndex) {
            int[] ints = Arrays.copyOf(bucketIndices, bucketIndices.length + 1);
            ints[ints.length - 1] = bucketIndex;
            return new CollatorAddress(ints);
        }

        @Override
        public String toString() {
            return '/' + Arrays.stream(bucketIndices).mapToObj(i -> "" + i).collect(Collectors.joining("/"));
        }
    }

    // TODO add stats from CollatorStatsAggregator
    public static final class CollationNodeInfo {
        final CollatorAddress address;
        final int numberOfBuckets, bitCount, bitOffset;
        final int bucketObjectCount;
        final long bucketSize;
        final boolean isSingleton;

        private CollationNodeInfo(CollatorAddress address, int numberOfBuckets, int bitCount, int bitOffset,
                                  int bucketObjectCount, final long bucketSize, boolean isSingleton) {
            this.address = address;
            this.numberOfBuckets = numberOfBuckets;
            this.bitCount = bitCount;
            this.bitOffset = bitOffset;
            this.bucketObjectCount = bucketObjectCount;
            this.bucketSize = bucketSize;
            this.isSingleton = isSingleton;
        }

        @Override
        public String toString() {
            return address + (isSingleton ? "S" : "N") + " (" + bitCount + "|" + bitOffset + "|" + numberOfBuckets + "): " +
                    " objs=" + bucketObjectCount +
                    " size=" + FormatUtils.bytesToString(bucketSize);
        }
    }

    static int minimalNumberOfBits(long size, long maxPartSize) {
        return 64 - Long.numberOfLeadingZeros((size + maxPartSize - 1) / maxPartSize - 1);
    }
}
