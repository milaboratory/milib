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

import com.milaboratory.primitivio.PrimitivO;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.util.io.AsynchronousFileChannelAdapter;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

/**
 * Helper class to mix synchronous PrimitivO writing with asynchronous / parallel PrimitivOBlocks serialization
 * for the same AsynchronousByteChannel
 */
public final class PrimitivOHybrid implements AutoCloseable {
    final ExecutorService executorService;
    final AsynchronousByteChannel byteChannel;
    PrimitivOState primitivOState;
    PrimitivO primitivO;
    PrimitivOBlocks.Writer primitivOBlocks;

    public PrimitivOHybrid(ExecutorService executorService, Path file) throws IOException {
        this(executorService, file, PrimitivOState.INITIAL);
    }

    public PrimitivOHybrid(ExecutorService executorService, Path file, PrimitivOState state) throws IOException {
        this(executorService, new AsynchronousFileChannelAdapter(
                        PrimitivIOBlocksAbstract.createAsyncChannel(executorService, file,
                                new OpenOption[0], StandardOpenOption.READ),
                        0),
                state);
    }

    public PrimitivOHybrid(ExecutorService executorService, AsynchronousByteChannel byteChannel, PrimitivOState state) {
        this.executorService = executorService;
        this.byteChannel = byteChannel;
        this.primitivOState = state;
    }

    public synchronized PrimitivO beginPrimitivO() {
        if (primitivOBlocks != null || primitivO != null)
            throw new IllegalStateException();
        return primitivO = primitivOState.createPrimitivO(Channels.newOutputStream(byteChannel));
    }

    public synchronized void endPrimitivO() {
        primitivOState = primitivO.getState();
        primitivO = null;
    }

    static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    static final LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();

    public synchronized <O> PrimitivOBlocks<O>.Writer beginPrimitivOBlocks(int concurrency, int blockSize) {
        if (primitivOBlocks != null || primitivO != null)
            throw new IllegalStateException();
        final PrimitivOBlocks<O> oPrimitivOBlocks = new PrimitivOBlocks<>(executorService, concurrency,
                primitivOState, blockSize, lz4Compressor);
        return primitivOBlocks = oPrimitivOBlocks.newWriter(byteChannel, false);
    }

    public synchronized void endPrimitivOBlocks() {
        primitivOBlocks.close();
        primitivOBlocks = null;
    }

    @Override
    public void close() throws IOException {
        byteChannel.close();
    }
}
