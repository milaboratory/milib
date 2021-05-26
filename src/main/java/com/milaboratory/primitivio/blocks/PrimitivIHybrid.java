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
import com.milaboratory.util.LambdaSemaphore;
import com.milaboratory.util.io.AsynchronousFileChannelAdapter;
import com.milaboratory.util.io.HasMutablePosition;
import com.milaboratory.util.io.HasPosition;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.io.input.CountingInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.milaboratory.primitivio.blocks.PrimitivIOBlocksUtil.defaultExecutorService;

// FIXME update docs with RandomAccess options

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
public final class PrimitivIHybrid implements HasMutablePosition, AutoCloseable {
    public static final int DEFAULT_PRIMITIVIO_BUFFER_SIZE = 524_288;

    // Base fields
    private boolean closed = false;
    private final ExecutorService executorService;
    private final AsynchronousFileChannelAdapter byteChannel;

    // Settings
    /** this value is used if readAheadBlocks is not specified */
    private int defaultReadAheadBlocks = 5;
    /** Concurrency limiter that will be inherited by all child readers */
    private final LambdaSemaphore concurrencyLimiter;

    /** Saved PrimitivI state */
    private PrimitivIState primitivIState;

    /** Exclusive PrimitivI */
    private PrimitivI continuousPrimitivI;
    /** List of non-exclusive */
    private final List<PrimitivI> randomAccessPrimitivI = new LinkedList<>();
    /** If set, exclusive primitivI state will be saved by by this object */
    private boolean saveStateAfterClose;

    /** Used to correctly track intermediate positions in buffered exclusive PrimitivI mode */
    private CountingInputStream countingInputStream;
    /** Position before initiation of exclusive PrimitivI session (see usage for details) */
    private long savedPosition;

    /** List of non-exclusive PrimitivIBlock readers */
    @SuppressWarnings("rawtypes")
    private final List<PrimitivIBlocks.Reader> randomAccessBlockReaders = new LinkedList<>();
    /** Exclusive PrimitivIBlock reader */
    @SuppressWarnings("rawtypes")
    private PrimitivIBlocks.Reader continuousBlocksReader;

    public PrimitivIHybrid(Path file, int concurrency) throws IOException {
        this(file, new LambdaSemaphore(concurrency));
    }

    public PrimitivIHybrid(Path file, LambdaSemaphore concurrencyLimiter) throws IOException {
        this(defaultExecutorService(), file, PrimitivIState.INITIAL, concurrencyLimiter);
    }

    public PrimitivIHybrid(Path file, PrimitivIState primitivIState, int concurrency) throws IOException {
        this(defaultExecutorService(), file, primitivIState, new LambdaSemaphore(concurrency));
    }

    public PrimitivIHybrid(Path file, PrimitivIState primitivIState, LambdaSemaphore concurrencyLimiter) throws IOException {
        this(defaultExecutorService(), file, primitivIState, concurrencyLimiter);
    }

    public PrimitivIHybrid(ExecutorService executorService, Path file, int concurrency) throws IOException {
        this(executorService, file, PrimitivIState.INITIAL, new LambdaSemaphore(concurrency));
    }

    public PrimitivIHybrid(ExecutorService executorService, Path file, PrimitivIState primitivIState, LambdaSemaphore concurrencyLimiter) throws IOException {
        this(executorService,
                new AsynchronousFileChannelAdapter(PrimitivIOBlocksAbstract.createAsyncChannel(
                        executorService, file, new OpenOption[0], StandardOpenOption.READ), 0),
                primitivIState, concurrencyLimiter);
    }

    private PrimitivIHybrid(ExecutorService executorService, AsynchronousFileChannelAdapter byteChannel, PrimitivIState primitivIState, LambdaSemaphore concurrencyLimiter) {
        this.executorService = executorService;
        this.byteChannel = byteChannel;
        this.primitivIState = primitivIState;
        this.concurrencyLimiter = concurrencyLimiter;
    }

    public PrimitivIHybrid setDefaultReadAheadBlocks(int defaultReadAheadBlocks) {
        this.defaultReadAheadBlocks = defaultReadAheadBlocks;
        return this;
    }

    @Override
    public long getPosition() {
        checkNullState(true, true);
        return byteChannel.getPosition();
    }

    /**
     * See {@link AsynchronousFileChannelAdapter#setPosition(long)}.
     */
    @Override
    public void setPosition(long newPosition) {
        checkNullState(true, true);
        byteChannel.setPosition(newPosition);
    }

    /** State assertion */
    private void checkNullState(boolean checkClosed, boolean allowRAMode) {
        if (closed && checkClosed)
            throw new IllegalArgumentException("closed");

        randomAccessBlockReaders.removeIf(reader -> reader.closed);

        randomAccessPrimitivI.removeIf(PrimitivI::isClosed);

        if (continuousBlocksReader != null && continuousBlocksReader.closed)
            continuousBlocksReader = null;

        if (continuousPrimitivI != null && continuousPrimitivI.isClosed()) {
            if (saveStateAfterClose)
                primitivIState = continuousPrimitivI.getState();

            // recover original stream position after buffered reading
            ((HasMutablePosition) byteChannel).setPosition(savedPosition + countingInputStream.getByteCount());
            countingInputStream = null;
            savedPosition = 0;

            continuousPrimitivI = null;
        }

        if (!allowRAMode && isInRandomAccessMode())
            throw new IllegalStateException("random access primitivI blocks or primitivI reader not closed");
        if (isInBlocksMode())
            throw new IllegalStateException("primitivI blocks not closed");
        if (isInPrimitivIMode())
            throw new IllegalStateException("primitivI not closed");
    }

    public synchronized boolean isInPrimitivIMode() {
        return continuousPrimitivI != null;
    }

    public synchronized boolean isInBlocksMode() {
        return continuousBlocksReader != null;
    }

    public synchronized boolean isInRandomAccessMode() {
        return !randomAccessBlockReaders.isEmpty() || !randomAccessPrimitivI.isEmpty();
    }

    public PrimitivI beginPrimitivI() {
        return beginPrimitivI(true, DEFAULT_PRIMITIVIO_BUFFER_SIZE);
    }

    public PrimitivI beginPrimitivI(boolean saveStateAfterClose) {
        return beginPrimitivI(saveStateAfterClose, DEFAULT_PRIMITIVIO_BUFFER_SIZE);
    }

    public synchronized PrimitivI beginPrimitivI(boolean saveStateAfterClose, int bufferSize) {
        checkNullState(true, false);

        this.saveStateAfterClose = saveStateAfterClose;
        this.savedPosition = ((HasPosition) byteChannel).getPosition();
        return continuousPrimitivI = primitivIState.createPrimitivI(
                countingInputStream = new CountingInputStream(
                        new BufferedInputStream(
                                new CloseShieldInputStream(
                                        Channels.newInputStream(byteChannel)), bufferSize)
                ));
    }

    public synchronized PrimitivI beginRandomAccessPrimitivI(long position) {
        PrimitivI primitivI = primitivIState.createPrimitivI(
                new CountingInputStream(
                        new BufferedInputStream(
                                new CloseShieldInputStream(
                                        Channels.newInputStream(byteChannel.createChildAdapter(position))),
                                DEFAULT_PRIMITIVIO_BUFFER_SIZE)
                ));
        randomAccessPrimitivI.add(primitivI);
        return primitivI;
    }

    static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

    //region beginPrimitivIBlocks

    public <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz) {
        return beginPrimitivIBlocks(clazz, defaultReadAheadBlocks, PrimitivIHeaderActions.skipAll());
    }

    public <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz, int readAheadBlocks) {
        return beginPrimitivIBlocks(clazz, readAheadBlocks, PrimitivIHeaderActions.skipAll());
    }

    public synchronized <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz, Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) {
        return beginPrimitivIBlocks(clazz, defaultReadAheadBlocks, specialHeaderAction);
    }

    public synchronized <O> PrimitivIBlocks<O>.Reader beginPrimitivIBlocks(Class<O> clazz, int readAheadBlocks,
                                                                           Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) {
        checkNullState(true, false);
        final PrimitivIBlocks<O> oPrimitivIBlocks = new PrimitivIBlocks<>(clazz, executorService, concurrencyLimiter, primitivIState, lz4Decompressor);
        final PrimitivIBlocks<O>.Reader reader = oPrimitivIBlocks.newReader(byteChannel, readAheadBlocks, specialHeaderAction, false);
        continuousBlocksReader = reader;
        return reader;
    }

    //endregion

    //region beginRandomAccessPrimitivIBlocks

    public <O> PrimitivIBlocks<O>.Reader beginRandomAccessPrimitivIBlocks(Class<O> clazz, long position) {
        return beginRandomAccessPrimitivIBlocks(clazz, position, defaultReadAheadBlocks, PrimitivIHeaderActions.skipAll());
    }

    public <O> PrimitivIBlocks<O>.Reader beginRandomAccessPrimitivIBlocks(Class<O> clazz, long position, int readAheadBlocks) {
        return beginRandomAccessPrimitivIBlocks(clazz, position, readAheadBlocks, PrimitivIHeaderActions.skipAll());
    }

    public <O> PrimitivIBlocks<O>.Reader beginRandomAccessPrimitivIBlocks(Class<O> clazz, long position,
                                                                          Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) {
        return beginRandomAccessPrimitivIBlocks(clazz, position, defaultReadAheadBlocks, specialHeaderAction);
    }

    public synchronized <O> PrimitivIBlocks<O>.Reader beginRandomAccessPrimitivIBlocks(Class<O> clazz, long position, int readAheadBlocks,
                                                                                       Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) {
        checkNullState(true, true);
        final PrimitivIBlocks<O> oPrimitivIBlocks = new PrimitivIBlocks<>(clazz, executorService, concurrencyLimiter, primitivIState, lz4Decompressor);
        PrimitivIBlocks<O>.Reader reader = oPrimitivIBlocks.newReader(byteChannel.createChildAdapter(position), readAheadBlocks, specialHeaderAction, false);
        randomAccessBlockReaders.add(reader);
        return reader;
    }

    //endregion

    @Override
    public void close() throws IOException {
        if (closed)
            return;

        checkNullState(false, false);

        closed = true;
        byteChannel.close();
    }
}
