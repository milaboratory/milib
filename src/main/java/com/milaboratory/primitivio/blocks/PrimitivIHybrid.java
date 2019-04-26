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

import com.milaboratory.primitivio.PrimitivI;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.util.io.IOUtil;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

/**
 * Helper class to mix synchronous PrimitivI reading with asynchronous / parallel PrimitivIBlocks streaming
 * for the same AsynchronousByteChannel
 */
public final class PrimitivIHybrid implements AutoCloseable {
    final ExecutorService executorService;
    final AsynchronousByteChannel byteChannel;
    PrimitivIState primitivIState;
    PrimitivI primitivI;
    PrimitivIBlocks.Reader primitivIBlocks;

    public PrimitivIHybrid(ExecutorService executorService, Path file) throws IOException {
        this(executorService, file, PrimitivIState.INITIAL);
    }

    public PrimitivIHybrid(ExecutorService executorService, Path file, PrimitivIState primitivIState) throws IOException {
        this(executorService,
                IOUtil.toAsynchronousByteChannel(
                        PrimitivIOBlocksAbstract.createAsyncChannel(executorService, file, new OpenOption[0], StandardOpenOption.READ),
                        0),
                primitivIState);
    }

    public PrimitivIHybrid(ExecutorService executorService, AsynchronousByteChannel byteChannel, PrimitivIState primitivIState) {
        this.executorService = executorService;
        this.byteChannel = byteChannel;
        this.primitivIState = primitivIState;
    }

    public synchronized PrimitivI beginPrimitivI() {
        if (primitivIBlocks != null || primitivI != null)
            throw new IllegalStateException();
        return primitivI = primitivIState.createPrimitivI(Channels.newInputStream(byteChannel));
    }

    public synchronized void endPrimitivI() {
        primitivIState = primitivI.getState();
        primitivI = null;
    }

    static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

    public synchronized <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz, int concurrency, int readAheadBlocks) {
        if (primitivIBlocks != null || primitivI != null)
            throw new IllegalStateException();
        final PrimitivIBlocks<O> oPrimitivIBlocks = new PrimitivIBlocks<>(executorService, concurrency, clazz, lz4Decompressor, primitivIState);
        return primitivIBlocks = oPrimitivIBlocks.newReader(byteChannel, readAheadBlocks, false);
    }

    public synchronized void endPrimitivIBlocks() {
        primitivIBlocks.close();
        primitivIBlocks = null;
    }

    @Override
    public void close() throws IOException {
        byteChannel.close();
    }
}
