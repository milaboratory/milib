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

import com.milaboratory.primitivio.PrimitivIState;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

public final class PrimitivIBlocks<O> extends PrimitivIOBlocksAbstract {
    /**
     * LZ4 compressor to compress data blocks
     */
    private final LZ4FastDecompressor decompressor;

    /**
     * PrimitivI stream state
     */
    private final PrimitivIState inputState;


    public PrimitivIBlocks(ExecutorService executor, int concurrency, LZ4FastDecompressor decompressor, PrimitivIState inputState) {
        super(executor, concurrency);
        this.decompressor = decompressor;
        this.inputState = inputState;
    }

    /**
     * Helper method to create async channel for reading with this object's execution service
     */
    public AsynchronousFileChannel createAsyncChannel(Path path, OpenOption... additionalOptions) throws IOException {
        return createAsyncChannel(path, additionalOptions, StandardOpenOption.READ);
    }

    public Reader newReader(Path channel, int readAheadBuffers) throws IOException {
        return new Reader(createAsyncChannel(channel), readAheadBuffers, 0, true);
    }

    public Reader newReader(AsynchronousFileChannel channel, int readAheadBuffers, long position) {
        return new Reader(channel, readAheadBuffers, position, false);
    }

    public final class Reader {
        final AsynchronousFileChannel channel;
        final int readAheadBuffers;
        final boolean closeUnderlyingChannel;
        volatile long position;
        final ConcurrentLinkedQueue<List<O>> blocks = new ConcurrentLinkedQueue<>();

        public Reader(AsynchronousFileChannel channel, int readAheadBuffers, long position, boolean closeUnderlyingChannel) {
            this.channel = channel;
            this.readAheadBuffers = readAheadBuffers;
            this.position = position;
            this.closeUnderlyingChannel = closeUnderlyingChannel;
        }
    }
}
