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
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.test.TestUtil;
import com.milaboratory.util.FormatUtils;
import com.milaboratory.util.RandomUtil;
import com.milaboratory.util.TempFileManager;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    public void benchmark1() throws IOException {
        for (int i = 0; i < 10; i++) {
            runTest(false, 1, 1000000, 1);
            // runTest(false, 2, 100000, 1);
            // runTest(false, 3, 100000, 1);
            runTest(false, 4, 1000000, 1);
            runTest(true, 1, 1000000, 2);
            // runTest(true, 2, 100000, 2);
            runTest(true, 4, 1000000, 2);
        }
    }

    final HashMap<Integer, byte[]> checksums = new HashMap<>();

    public void runTest(boolean highCompression,
                        int concurrency,
                        int elements,
                        int checksumSlot) throws IOException {
        // LZ4Factory lz4Factory = LZ4Factory.fastestJavaInstance();
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = highCompression
                ? lz4Factory.highCompressor(1)
                : lz4Factory.fastCompressor();

        Path target = TempFileManager.getTempFile().toPath();

        PrimitivOBlocks<SingleRead> o = new PrimitivOBlocks<>(executorService, concurrency, PrimitivOState.INITIAL, 1024, compressor
        );

        RandomUtil.reseedThreadLocal(12341);

        List<SingleRead> sr = new ArrayList<>();

        int elementsInRepeat = 1000;
        int repeats = elements / elementsInRepeat;

        for (int i = 0; i < elementsInRepeat; i++) {
            NucleotideSequence seq = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 100, 2000);
            SingleReadImpl test = new SingleReadImpl(0, new NSequenceWithQuality(seq), "Test");
            sr.add(test);
        }

        long startTimestamp = System.nanoTime();
        o.resetStats();
        try (PrimitivOBlocks<SingleRead>.Writer writer = o.newWriter(target)) {
            for (int i = 0; i < repeats; i++)
                for (int j = 0; j < elementsInRepeat; j++)
                    writer.write(sr.get(j));
        }
        long elapsed = System.nanoTime() - startTimestamp;
        System.out.println();
        System.out.println("==================");
        System.out.println("High compression: " + highCompression);
        System.out.println("Concurrency: " + concurrency);
        System.out.println("File size: " + Files.size(target));
        System.out.println("Write time: " + FormatUtils.nanoTimeToString(elapsed));
        System.out.println("Stats:");
        System.out.println(o.getStats());

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
        else
            Assert.assertArrayEquals(checksum, expectedChecksum);

        Files.delete(target);
    }
}