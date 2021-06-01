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
import cc.redberry.pipe.util.CountLimitingOutputPort;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.test.TestUtil;
import com.milaboratory.util.TempFileManager;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class HashSorterTest {
    @Test
    public void numberOfBits() {
        Assert.assertEquals(1, HashSorter.minimalNumberOfBits(10, 5));
        Assert.assertEquals(1, HashSorter.minimalNumberOfBits(10, 7));
        Assert.assertEquals(2, HashSorter.minimalNumberOfBits(10, 4));
        Assert.assertEquals(0, HashSorter.minimalNumberOfBits(10, 10));
    }

    @Test
    public void testMapping1() {
        HashSorter.BucketMapping<Long> mapping = new HashSorter.BucketMapping<>(
                Long::intValue, Comparator.<Long>naturalOrder(),
                4, 0, new Object[0]);
        Assert.assertEquals(16, mapping.getNumberOfBuckets());
        for (int i = 0; i < 16; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i));
            Assert.assertEquals(i, mapping.getBucketId((long) i));
        }
    }

    @Test
    public void testMapping2() {
        HashSorter.BucketMapping<Long> mapping = new HashSorter.BucketMapping<>(
                Long::intValue, Comparator.<Long>naturalOrder(),
                4, 0, new Object[]{3 + 16L});
        Assert.assertEquals(18, mapping.getNumberOfBuckets());
        for (int i = 0; i < 4; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i));
            Assert.assertEquals(i, mapping.getBucketId((long) i));
        }
        for (int i = 4; i < 16; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i + 2));
            Assert.assertEquals(i + 2, mapping.getBucketId((long) i));
        }
        Assert.assertEquals(4, mapping.getBucketId(3 + 16L));
    }

    @Test
    public void testMapping3() {
        HashSorter.BucketMapping<Long> mapping = new HashSorter.BucketMapping<>(
                Long::intValue, Comparator.<Long>naturalOrder(),
                4, 0, new Object[]{3 + 16L, 3 + 3 * 16L});
        Assert.assertEquals(20, mapping.getNumberOfBuckets());
        for (int i = 0; i < 4; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i));
            Assert.assertEquals(i, mapping.getBucketId((long) i));
        }
        for (int i = 4; i < 16; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i + 4));
            Assert.assertEquals(i + 4, mapping.getBucketId((long) i));
        }
        Assert.assertTrue(mapping.isSingletonBucket(4));
        Assert.assertEquals(4, mapping.getBucketId(3 + 16L));
        Assert.assertFalse(mapping.isSingletonBucket(5));
        Assert.assertEquals(5, mapping.getBucketId(3 + 2 * 16L));
        Assert.assertTrue(mapping.isSingletonBucket(6));
        Assert.assertEquals(6, mapping.getBucketId(3 + 3 * 16L));
        Assert.assertFalse(mapping.isSingletonBucket(7));
        Assert.assertEquals(7, mapping.getBucketId(3 + 4 * 16L));
    }

    @Test
    public void testMapping4() {
        HashSorter.BucketMapping<Long> mapping = new HashSorter.BucketMapping<>(
                Long::intValue, Comparator.<Long>naturalOrder(),
                4, 0, new Object[]{3 + 16L, 4 + 3 * 16L});
        Assert.assertEquals(20, mapping.getNumberOfBuckets());
        for (int i = 0; i < 4; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i));
            Assert.assertEquals(i, mapping.getBucketId((long) i));
        }
        Assert.assertFalse(mapping.isSingletonBucket(4 + 2));
        Assert.assertEquals(4 + 2, mapping.getBucketId((long) 4));
        for (int i = 5; i < 16; i++) {
            Assert.assertFalse(mapping.isSingletonBucket(i + 4));
            Assert.assertEquals(i + 4, mapping.getBucketId((long) i));
        }
        Assert.assertTrue(mapping.isSingletonBucket(4));
        Assert.assertEquals(4, mapping.getBucketId(3 + 16L));
        Assert.assertFalse(mapping.isSingletonBucket(5));
        Assert.assertEquals(5, mapping.getBucketId(3 + 2 * 16L));
        Assert.assertFalse(mapping.isSingletonBucket(6));
        Assert.assertEquals(6, mapping.getBucketId(4 + 16L));
        Assert.assertTrue(mapping.isSingletonBucket(7));
        Assert.assertEquals(7, mapping.getBucketId(4 + 3 * 16L));
        Assert.assertFalse(mapping.isSingletonBucket(8));
        Assert.assertEquals(8, mapping.getBucketId(4 + 4 * 16L));
    }

    @Test
    public void test1() {
        List<NucleotideSequence> seqsList = new ArrayList<>();
        for (int i = 0; i < 1 << 15; i++)
            seqsList.add(TestUtil.randomSequence(NucleotideSequence.ALPHABET, 20, 200));

        RandomGenerator rg = new Well19937c(1234);

        int N = 5000000;
        AtomicInteger unorderedHash = new AtomicInteger(0);
        OutputPort<NucleotideSequence> seqs = new OutputPort<NucleotideSequence>() {
            @Override
            public synchronized NucleotideSequence take() {
                NucleotideSequence seq = seqsList.get(rg.nextInt(seqsList.size()));
                unorderedHash.accumulateAndGet(seq.hashCode(), (left, right) -> left + right);
                return seq;
            }
        };
        seqs = new CountLimitingOutputPort<>(seqs, N);

        File dir = TempFileManager.getTempDir();
        System.out.println(dir);

        HashSorter<NucleotideSequence> c = new HashSorter<>(
                NucleotideSequence.class,
                Objects::hashCode, Comparator.naturalOrder(),
                5, dir.toPath(), 4, 6,
                PrimitivOState.INITIAL, PrimitivIState.INITIAL,
                1 << 23, 128);

        Comparator<NucleotideSequence> ec = c.getEffectiveComparator();

        try (OutputPortCloseable<NucleotideSequence> port = c.port(seqs)) {
            long actualN = 0;
            int uh = unorderedHash.get();
            NucleotideSequence previous = null;
            for (NucleotideSequence ns : CUtils.it(port)) {
                ++actualN;
                uh -= ns.hashCode();
                if (previous != null) {
                    int compare = ec.compare(previous, ns);
                    Assert.assertTrue(compare <= 0);
                }
                previous = ns;
            }

            Assert.assertEquals(N, actualN);
            Assert.assertEquals(0, uh);

            c.printStat();
        }
    }

    @Test
    public void test2() {
        TestUtil.assumeLongTest();

        List<NucleotideSequence> seqsList = new ArrayList<>();
        for (int i = 0; i < 15; i++)
            seqsList.add(TestUtil.randomSequence(NucleotideSequence.ALPHABET, 15000, 20000));

        RandomGenerator rg = new Well19937c(1234);

        int N = 500000;
        AtomicInteger unorderedHash = new AtomicInteger(0);
        OutputPort<NucleotideSequence> seqs = new OutputPort<NucleotideSequence>() {
            @Override
            public synchronized NucleotideSequence take() {
                NucleotideSequence seq = seqsList.get(rg.nextInt(seqsList.size()));
                unorderedHash.accumulateAndGet(seq.hashCode(), Integer::sum);
                return seq;
            }
        };
        seqs = new CountLimitingOutputPort<>(seqs, N);

        File dir = TempFileManager.getTempDir();
        System.out.println(dir);

        HashSorter<NucleotideSequence> c = new HashSorter<>(
                NucleotideSequence.class,
                Objects::hashCode, Comparator.naturalOrder(),
                5, dir.toPath(), 4, 6,
                PrimitivOState.INITIAL, PrimitivIState.INITIAL,
                1 << 20, 1 << 15);

        Comparator<NucleotideSequence> ec = c.getEffectiveComparator();

        try (OutputPortCloseable<NucleotideSequence> port = c.port(seqs)) {
            long actualN = 0;
            int uh = unorderedHash.get();
            NucleotideSequence previous = null;
            for (NucleotideSequence ns : CUtils.it(port)) {
                ++actualN;
                uh -= ns.hashCode();
                if (previous != null) {
                    int compare = ec.compare(previous, ns);
                    Assert.assertTrue(compare <= 0);
                }
                previous = ns;
            }

            Assert.assertEquals(N, actualN);
            Assert.assertEquals(0, uh);

            c.printStat();
        }
    }

    @Test
    public void testSingleton() {
        NucleotideSequence seq = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 15000, 20000);

        int N = 50000;
        AtomicInteger unorderedHash = new AtomicInteger(0);
        OutputPort<NucleotideSequence> seqs = new OutputPort<NucleotideSequence>() {
            @Override
            public synchronized NucleotideSequence take() {
                return seq;
            }
        };
        seqs = new CountLimitingOutputPort<>(seqs, N);

        File dir = TempFileManager.getTempDir();
        System.out.println(dir);

        HashSorter<NucleotideSequence> c = new HashSorter<>(
                NucleotideSequence.class,
                Objects::hashCode, Comparator.naturalOrder(),
                5, dir.toPath(), 4, 6,
                PrimitivOState.INITIAL, PrimitivIState.INITIAL,
                1 << 20, 128);

        Comparator<NucleotideSequence> ec = c.getEffectiveComparator();

        try (OutputPortCloseable<NucleotideSequence> port = c.port(seqs)) {
            long actualN = 0;
            for (NucleotideSequence ignored : CUtils.it(port))
                ++actualN;

            Assert.assertEquals(N, actualN);

            c.printStat();
            Assert.assertEquals(1, c.getNumberOfNodes());
        }
    }
}
