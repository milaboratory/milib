/*
 * Copyright 2019 MiLaboratory, LLC
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
package com.milaboratory.primitivio.blocks;

import com.milaboratory.core.io.sequence.SingleRead;
import com.milaboratory.core.io.sequence.SingleReadImpl;
import com.milaboratory.core.sequence.NSequenceWithQuality;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.primitivio.PrimitivI;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.primitivio.PrimitivO;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.test.TestUtil;
import com.milaboratory.util.FormatUtils;
import com.milaboratory.util.HashFunctions;
import com.milaboratory.util.RandomUtil;
import com.milaboratory.util.TempFileManager;
import com.milaboratory.util.io.HasPosition;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.math3.random.Well19937a;
import org.junit.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PrimitivOBlocksTest {
    static ExecutorService executorService;

    @BeforeClass
    public static void setUp() throws Exception {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        executorService.shutdownNow();
    }

    @Test
    public void test1() throws IOException {
        AtomicInteger counter = new AtomicInteger();
        HashMap<Integer, Integer> cSlots = new HashMap<>();
        for (int elements : new int[]{TestUtil.its(20000, 100000)})
            for (boolean highCompression : new boolean[]{false, true})
                for (int concurrency : new int[]{4, 1})
                    for (int blockSize : new int[]{172, 1024}) {
                        Integer slot = cSlots.computeIfAbsent(Objects.hash(highCompression, blockSize, elements),
                                q -> counter.incrementAndGet());
                        runTest(highCompression, concurrency, elements, slot, blockSize);
                    }
    }

    @Test
    @Ignore
    public void benchmark1() throws IOException {
        AtomicInteger counter = new AtomicInteger();
        HashMap<Integer, Integer> cSlots = new HashMap<>();
        for (int elements : new int[]{1000000, 100000})
            // for (int elements : new int[]{100000})
            for (boolean highCompression : new boolean[]{false, true})
                for (int concurrency : new int[]{4, 1})
                    for (int blockSize : new int[]{172, 1024}) {
                        Integer slot = cSlots.computeIfAbsent(Objects.hash(highCompression, blockSize, elements),
                                q -> counter.incrementAndGet());
                        runTest(highCompression, concurrency, elements, slot, blockSize);
                    }
    }

    final HashMap<Integer, byte[]> checksums = new HashMap<>();

    public void runTest(boolean highCompression,
                        int concurrency,
                        int elements,
                        int checksumSlot,
                        int blockSize) throws IOException {
        // Opening the latch will result in monitoring thread termination
        CountDownLatch pendingOpsPrinterLatch = new CountDownLatch(1);
        AtomicReference<PrimitivIOBlocksAbstract> statSource = new AtomicReference<>();
        new Thread(() -> {
            try {
                while (!pendingOpsPrinterLatch.await(2, TimeUnit.SECONDS)) {
                    PrimitivIOBlocksAbstract io = statSource.get();
                    if (io == null)
                        continue;
                    PrimitivIOBlocksStatsAbstract stats = io.getStats();
                    System.out.println("Pending / IO / Serde: " + stats.getPendingOps() +
                            " / " + stats.getOngoingIOOps() +
                            " / " + stats.getOngoingSerdes());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // LZ4Factory lz4Factory = LZ4Factory.fastestJavaInstance();
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = highCompression
                ? lz4Factory.highCompressor()
                : lz4Factory.fastCompressor();

        Path target = TempFileManager.getTempFile().toPath();
        Path hybridTarget = TempFileManager.getTempFile().toPath();

        PrimitivOBlocks<SingleRead> io = new PrimitivOBlocks<>(executorService, concurrency,
                PrimitivOState.INITIAL, blockSize, compressor);

        RandomUtil.reseedThreadLocal(12341);

        List<SingleRead> sr = new ArrayList<>();

        int elementsInRepeat = 1000;
        int repeats = elements / elementsInRepeat;

        for (int i = 0; i < elementsInRepeat; i++) {
            NucleotideSequence seq = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 100, 2000);
            SingleReadImpl test = new SingleReadImpl(0, new NSequenceWithQuality(seq), "Test");
            sr.add(test);
        }

        RandomUtil.reseedThreadLocal(12341);

        long startTimestamp = System.nanoTime();
        io.resetStats();

        statSource.set(io);

        ConcurrentLinkedQueue<Long> anchorPositions = new ConcurrentLinkedQueue<>();

        long k = 0;
        try (
                PrimitivOBlocks<SingleRead>.Writer writer = io.newWriter(target);
                PrimitivOHybrid ho = new PrimitivOHybrid(executorService, hybridTarget)) {

            writer.run(c -> anchorPositions.offer(((HasPosition) c).getPosition()));

            // Raw Blocks io
            for (int i = 0; i < repeats; i++) {
                int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);
                for (int j = 0; j < els; j++)
                    writer.write(sr.get(j));
                writer.flush();
                writer.run(c -> anchorPositions.offer(((HasPosition) c).getPosition()));
                writer.writeHeader(PrimitivIOBlockHeader.specialHeader().setSpecialLong(0, k += 124));
                writer.run(c -> anchorPositions.offer(((HasPosition) c).getPosition()));
            }

            writer.sync();

            Assert.assertEquals(Long.valueOf(0), anchorPositions.poll());
            Assert.assertEquals(repeats * 2, anchorPositions.size());

            // Hybrid
            for (int i = 0; i < 2; i++) {
                // no blocks
                try (PrimitivO primitivO = ho.beginPrimitivO()) {
                    int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);
                    for (int j = 0; j < els; j++)
                        primitivO.writeObject(sr.get(j));
                }

                int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);

                // blocks
                try (final PrimitivOBlocks<SingleRead>.Writer bWriter = ho.<SingleRead>beginPrimitivOBlocks(concurrency, blockSize)) {
                    for (int j = 0; j < els; j++)
                        bWriter.write(sr.get(j));
                }
            }
        }
        long elapsed = System.nanoTime() - startTimestamp;
        System.out.println();
        System.out.println("==================");
        System.out.println("High compression: " + highCompression);
        System.out.println("Concurrency: " + concurrency);
        System.out.println("File size: " + Files.size(target));
        System.out.println("Write time: " + FormatUtils.nanoTimeToString(elapsed));
        System.out.println();
        System.out.println("O. Stats:");
        System.out.println(io.getStats());

        byte[] checksum;
        try (InputStream in = new FileInputStream(target.toFile())) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] block = new byte[4096];
            int length;
            while ((length = in.read(block)) > 0) {
                digest.update(block, 0, length);
            }
            checksum = digest.digest();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        byte[] expectedChecksum = checksums.get(checksumSlot);
        if (expectedChecksum == null)
            checksums.put(checksumSlot, checksum);
        else {
            Assert.assertArrayEquals(checksum, expectedChecksum);
            System.out.println("Checksum ok!");
        }

        RandomUtil.reseedThreadLocal(12341);

        PrimitivIBlocks<SingleRead> pi = new PrimitivIBlocks<>(SingleRead.class, executorService, concurrency,
                PrimitivIState.INITIAL, lz4Factory.fastDecompressor());

        statSource.set(pi);

        try (PrimitivIBlocks<SingleRead>.Reader reader = pi.newReader(target, 2)) {
            for (int i = 0; i < repeats; i++) {
                int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);
                for (int j = 0; j < els; j++) {
                    final SingleRead obj = reader.take();
                    Assert.assertEquals(sr.get(j), obj);
                }
            }
            Assert.assertNull(reader.take());
        }

        System.out.println();
        System.out.println("I. Stats 1:");
        System.out.println(pi.getStats());

        RandomUtil.reseedThreadLocal(12341);

        k = 0;
        try (
                PrimitivIBlocks<SingleRead>.Reader reader = pi.newReader(target, 2,
                        h -> PrimitivIHeaderActions.outputObject(
                                new SingleReadImpl(h.getSpecialLong(0), NSequenceWithQuality.EMPTY, "")));
                PrimitivIHybrid hi =
                        new PrimitivIHybrid(executorService, hybridTarget, 2)
                                .setDefaultReadAheadBlocks(2)) {

            // Raw Blocks io
            for (int i = 0; i < repeats; i++) {
                int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);
                for (int j = 0; j < els; j++) {
                    final SingleRead obj = reader.take();
                    Assert.assertEquals(sr.get(j), obj);
                }

                // Header-produced object
                final SingleRead obj = reader.take();
                Assert.assertEquals(new SingleReadImpl(k += 124, NSequenceWithQuality.EMPTY, ""), obj);
            }
            Assert.assertNull(reader.take());

            // Hybrid
            for (int i = 0; i < 2; i++) {
                // no blocks
                try (PrimitivI primitivI = hi.beginPrimitivI(false)) {
                    int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);
                    for (int j = 0; j < els; j++) {
                        SingleRead obj = primitivI.readObject(SingleRead.class);
                        Assert.assertEquals(sr.get(j), obj);
                    }
                }

                long blocksBegin = hi.getPosition();

                int els = 1 + RandomUtil.getThreadLocalRandom().nextInt(elementsInRepeat - 1);

                // blocks
                try (PrimitivIBlocks<SingleRead>.Reader bReader =
                             hi.<SingleRead>beginPrimitivIBlocks(SingleRead.class)) {
                    for (int j = 0; j < els; j++) {
                        SingleRead obj = bReader.take();
                        Assert.assertEquals(sr.get(j), obj);
                    }
                }

                try (
                        PrimitivIBlocks<SingleRead>.Reader bReader1 =
                                hi.<SingleRead>beginRandomAccessPrimitivIBlocks(SingleRead.class, blocksBegin);
                        PrimitivIBlocks<SingleRead>.Reader bReader2 =
                                hi.<SingleRead>beginRandomAccessPrimitivIBlocks(SingleRead.class, blocksBegin)
                ) {
                    for (int j = 0; j < els; j++) {
                        SingleRead obj = bReader1.take();
                        Assert.assertEquals(sr.get(j), obj);
                    }
                    for (int j = 0; j < els; j++) {
                        SingleRead obj = bReader2.take();
                        Assert.assertEquals(sr.get(j), obj);
                    }
                }
            }
        }

        System.out.println();
        System.out.println("I. Stats 2:");
        System.out.println(pi.getStats());

        Files.delete(target);
        Files.delete(hybridTarget);

        pendingOpsPrinterLatch.countDown();
    }

    @Test
    public void bigBlocks() throws IOException {
        // Opening the latch will result in monitoring thread termination
        CountDownLatch pendingOpsPrinterLatch = new CountDownLatch(1);
        AtomicReference<PrimitivIOBlocksAbstract> statSource = new AtomicReference<>();
        new Thread(() -> {
            try {
                while (!pendingOpsPrinterLatch.await(2, TimeUnit.SECONDS)) {
                    PrimitivIOBlocksAbstract io = statSource.get();
                    if (io == null)
                        continue;
                    PrimitivIOBlocksStatsAbstract stats = io.getStats();
                    System.out.println("Pending / IO / Serde / Objs: " + stats.getPendingOps() +
                            " / " + stats.getOngoingIOOps() +
                            " / " + stats.getOngoingSerdes() +
                            " / " + stats.getObjectCount());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        Path target = TempFileManager.getTempFile().toPath();

        PrimitivOBlocks<byte[]> io = new PrimitivOBlocks<byte[]>(executorService,
                8, PrimitivOState.INITIAL,
                100000, LZ4Factory.fastestInstance().fastCompressor());

        io.resetStats();

        statSource.set(io);

        long k = 0;
        try (PrimitivOBlocks<byte[]>.Writer writer = io.newWriter(target)) {
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 1000; j++) {
                    byte[] buffer = new byte[100000];
                    for (int i1 = 0; i1 < buffer.length; i1 += 8) {
                        ++k;
                        long s = HashFunctions.JenkinWang64shift(k);
                        buffer[i1] = (byte) (s & 0xFF);
                        buffer[i1 + 1] = (byte) ((s >> 8) & 0xFF);
                        buffer[i1 + 2] = (byte) ((s >> 16) & 0xFF);
                        buffer[i1 + 3] = (byte) ((s >> 24) & 0xFF);
                        buffer[i1 + 4] = (byte) ((s >> 32) & 0xFF);
                        buffer[i1 + 5] = (byte) ((s >> 40) & 0xFF);
                        buffer[i1 + 6] = (byte) ((s >> 48) & 0xFF);
                        buffer[i1 + 7] = (byte) ((s >> 54) & 0xFF);
                    }
                    writer.write(buffer);
                }
                writer.flush();
            }
        }

        System.out.println("O. Stats:");
        System.out.println(io.getStats());

        pendingOpsPrinterLatch.countDown();
    }
}
