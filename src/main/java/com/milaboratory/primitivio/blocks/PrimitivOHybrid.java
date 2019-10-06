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
import org.apache.commons.io.output.CloseShieldOutputStream;

import java.io.BufferedOutputStream;
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
 *
 * <pre>
 *  try(PrimitivOHybrid o = new PrimitivOHybrid(...)) {
 *      try(PrimitivO po = o.beginPrimitivO(true)){
 *          ....
 *      }
 *      try(PrimitivOBlocks<...>.Writer pob = o.<...>beginPrimitivOBlocks(..., ...)){
 *          ....
 *      }
 *  }
 * </pre>
 */
public final class PrimitivOHybrid implements AutoCloseable {
    private boolean closed = false;
    private final ExecutorService executorService;
    private final AsynchronousByteChannel byteChannel;

    private PrimitivOState primitivOState;

    private PrimitivO primitivO;
    private boolean saveStateAfterClose;

    private PrimitivOBlocks.Writer primitivOBlocks;

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

    private void checkNullState(boolean checkClosed) {
        if (closed && checkClosed)
            throw new IllegalArgumentException("closed");
        if (primitivOBlocks != null && primitivOBlocks.closed)
            primitivOBlocks = null;
        if (primitivO != null && primitivO.isClosed()) {
            if (saveStateAfterClose)
                primitivOState = primitivO.getState();
            primitivO = null;
        }
        if (primitivOBlocks != null)
            throw new IllegalStateException("primitivO blocks not closed");
        if (primitivO != null)
            throw new IllegalStateException("primitivO not closed");
    }

    public synchronized PrimitivO beginPrimitivO() {
        return beginPrimitivO(false);
    }

    /**
     * Enters synchronous primitivO mode
     *
     * @param saveStateAfterClose if true, state of returned primitivO, after it will be closed, will be saved;
     *                            all subsequent block / non-block writers will inherit the state
     * @return
     */
    public synchronized PrimitivO beginPrimitivO(boolean saveStateAfterClose) {
        checkNullState(true);

        this.saveStateAfterClose = saveStateAfterClose;
        return primitivO = primitivOState.createPrimitivO(
                new BufferedOutputStream( // Buffering here is even more important than with normal OutputStreams as channel synchronization is expensive
                        new CloseShieldOutputStream( // Preventing channel close via OutputStream.close by CloseShieldOutputStream
                                Channels.newOutputStream(byteChannel))));
    }

    static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    static final LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();

    public synchronized <O> PrimitivOBlocks<O>.Writer beginPrimitivOBlocks(int concurrency, int blockSize) {
        checkNullState(true);
        final PrimitivOBlocks<O> oPrimitivOBlocks = new PrimitivOBlocks<>(executorService, concurrency,
                primitivOState, blockSize, lz4Compressor);
        //noinspection unchecked
        return primitivOBlocks = oPrimitivOBlocks.newWriter(byteChannel, false);
    }

    @Override
    public void close() throws IOException {
        if (closed)
            return;

        checkNullState(false);

        closed = true;
        byteChannel.close();
    }
}
