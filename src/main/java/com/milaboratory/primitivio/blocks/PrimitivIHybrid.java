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
import com.milaboratory.util.io.AsynchronousFileChannelAdapter;
import com.milaboratory.util.io.HasMutablePosition;
import com.milaboratory.util.io.HasPosition;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.io.input.CountingInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * Helper class to mix synchronous PrimitivI reading with asynchronous / parallel PrimitivIBlocks streaming
 * for the same AsynchronousByteChannel
 *
 * <pre>
 *  try(PrimitivIHybrid i = new PrimitivIHybrid(...)) {
 *      try(PrimitivI pi = o.beginPrimitivI()){
 *          ....
 *      }
 *      try(PrimitivIBlocks<...>.Writer pib = o.<...>beginPrimitivIBlocks(..., ...)){
 *          ....
 *      }
 *  }
 * </pre>
 */
public final class PrimitivIHybrid implements HasPosition, AutoCloseable {
    public static final int DEFAULT_PRIMITIVIO_BUFFER_SIZE = 524_288;

    private boolean closed = false;
    private final ExecutorService executorService;
    private final AsynchronousByteChannel byteChannel;

    private PrimitivIState primitivIState;
    private PrimitivI primitivI;
    private boolean saveStateAfterClose;

    private CountingInputStream countingInputStream;
    private long savedPosition;

    private PrimitivIBlocks.Reader primitivIBlocks;

    public PrimitivIHybrid(ExecutorService executorService, Path file) throws IOException {
        this(executorService, file, PrimitivIState.INITIAL);
    }

    public PrimitivIHybrid(ExecutorService executorService, Path file, PrimitivIState primitivIState) throws IOException {
        this(executorService,
                new AsynchronousFileChannelAdapter(PrimitivIOBlocksAbstract.createAsyncChannel(
                        executorService, file, new OpenOption[0], StandardOpenOption.READ), 0),
                primitivIState);
    }

    private PrimitivIHybrid(ExecutorService executorService, AsynchronousByteChannel byteChannel, PrimitivIState primitivIState) {
        this.executorService = executorService;
        this.byteChannel = byteChannel;
        this.primitivIState = primitivIState;
    }

    @Override
    public long getPosition() {
        return ((HasPosition) byteChannel).getPosition();
    }

    private void checkNullState(boolean checkClosed) {
        if (closed && checkClosed)
            throw new IllegalArgumentException("closed");
        if (primitivIBlocks != null && primitivIBlocks.closed)
            primitivIBlocks = null;
        if (primitivI != null && primitivI.isClosed()) {
            if (saveStateAfterClose)
                primitivIState = primitivI.getState();

            // recover original stream position after buffered reading
            ((HasMutablePosition) byteChannel).setPosition(savedPosition + countingInputStream.getByteCount());
            countingInputStream = null;
            savedPosition = 0;

            primitivI = null;
        }
        if (primitivIBlocks != null)
            throw new IllegalStateException("primitivI blocks not closed");
        if (primitivI != null)
            throw new IllegalStateException("primitivI not closed");
    }

    public PrimitivI beginPrimitivI() {
        return beginPrimitivI(false);
    }

    public synchronized PrimitivI beginPrimitivI(boolean saveStateAfterClose) {
        checkNullState(true);

        this.saveStateAfterClose = saveStateAfterClose;
        this.savedPosition = ((HasPosition) byteChannel).getPosition();
        return primitivI = primitivIState.createPrimitivI(
                countingInputStream = new CountingInputStream(
                        new BufferedInputStream(
                                new CloseShieldInputStream(
                                        Channels.newInputStream(byteChannel)),
                                DEFAULT_PRIMITIVIO_BUFFER_SIZE)
                )
        );
    }

    static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

    public synchronized <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz, int concurrency, int readAheadBlocks) {
        return beginPrimitivIBlocks(clazz, concurrency, readAheadBlocks, PrimitivIHeaderActions.skipAll());
    }

    public synchronized <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz, int concurrency, int readAheadBlocks,
                                                                           Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) {
        checkNullState(true);
        final PrimitivIBlocks<O> oPrimitivIBlocks = new PrimitivIBlocks<>(clazz, executorService, concurrency, primitivIState, lz4Decompressor);
        //noinspection unchecked
        return primitivIBlocks = oPrimitivIBlocks.newReader(byteChannel, readAheadBlocks, specialHeaderAction,
                false);
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
