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

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PrimitivIOBlocksAbstract {
    protected static final int HASH_SEED = 0xD5D20F71;
    protected static final int BLOCK_HEADER_SIZE = 17;

    protected final AtomicInteger
            ongoingSerdes = new AtomicInteger(),
            ongoingIOOps = new AtomicInteger(),
            pendingOps = new AtomicInteger();

    /**
     * Number of active readers and writers
     */
    protected final AtomicInteger
            activeRWs = new AtomicInteger();

    /**
     * Executor for the deserialization routines
     */
    final ExecutorService executor;

    /**
     * LZ4 hash function
     */
    protected final XXHash32 xxHash32 = XXHashFactory.fastestJavaInstance().hash32();

    /**
     * Concurrency
     */
    protected final int concurrency;

    /**
     * Signal the error in one of the asynchronous actions
     */
    protected volatile Throwable exception = null;

    public PrimitivIOBlocksAbstract(ExecutorService executor, int concurrency) {
        if (concurrency <= 0)
            throw new IllegalArgumentException("concurrency must be a positive integer");
        this.executor = executor;
        this.concurrency = concurrency;
        runStatReporterIfDebug();
    }

    protected void _ex(Throwable ex) {
        if (exception != null)
            return;
        exception = ex;
    }

    /**
     * To be executed from user-facing function.
     *
     * Must not be executed inside async handlers.
     */
    protected void checkException() {
        if (exception != null)
            if (exception instanceof RuntimeException)
                throw (RuntimeException) exception;
            else
                throw new RuntimeException(exception);
    }

    public static AsynchronousFileChannel createAsyncChannel(ExecutorService executor, Path path,
                                                             OpenOption[] additionalOptions, OpenOption... options) throws IOException {
        Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
        opts.addAll(Arrays.asList(additionalOptions));
        return AsynchronousFileChannel.open(path, opts, executor);
    }

    public AsynchronousFileChannel createAsyncChannel(Path path, OpenOption[] additionalOptions, OpenOption... options) throws IOException {
        return createAsyncChannel(executor, path, additionalOptions, options);
    }

    public abstract PrimitivIOBlocksStatsAbstract getStats();

    private static final AtomicLong ID_COUNTER;
    private static final AtomicLong ACTIVE_REPORTERS = new AtomicLong();

    static {
        AtomicLong counter = null;
        try {
            String debugDefined = System.getenv("PRIMITIVIO_DEBUG");
            if (debugDefined != null)
                counter = new AtomicLong();
        } catch (Exception e) {
        }
        ID_COUNTER = counter;
    }

    /**
     * ID for debugging
     */
    private final long id = ID_COUNTER == null ? -1 : ID_COUNTER.getAndIncrement();

    protected void runStatReporterIfDebug() {
        if (id >= 0 && ACTIVE_REPORTERS.get() < 1) {
            ACTIVE_REPORTERS.incrementAndGet();
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean lastNoRW = false;
                        while (true) {
                            Thread.sleep(100);
                            if (lastNoRW && activeRWs.get() == 0)
                                return;
                            PrimitivIOBlocksStatsAbstract stats = getStats();
                            System.out.println("IO: " + id);
                            System.out.println(stats.toString());
                            Thread.sleep(10000);
                            lastNoRW = activeRWs.get() == 0;
                        }
                    } catch (Exception e) {
                    } finally {
                        ACTIVE_REPORTERS.decrementAndGet();
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
        }
    }
}
