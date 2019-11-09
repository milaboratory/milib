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
package com.milaboratory.util.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Partial adapter between AsynchronousFileChannel and AsynchronousByteChannel.
 */
public final class AsynchronousFileChannelAdapter implements AsynchronousByteChannel, HasMutablePosition {
    private final AsynchronousFileChannel fileChannel;
    private final AtomicLong positionCounter;
    private final boolean closeUnderlyingFileChannel;
    private volatile boolean closed = false;
    private final AtomicBoolean hasPendingOperation = new AtomicBoolean(false);

    public AsynchronousFileChannelAdapter(AsynchronousFileChannel fileChannel, long position) {
        this(fileChannel, position, true);
    }

    private AsynchronousFileChannelAdapter(AsynchronousFileChannel fileChannel, long position,
                                           boolean closeUnderlyingFileChannel) {
        this.fileChannel = fileChannel;
        this.positionCounter = new AtomicLong(position);
        this.closeUnderlyingFileChannel = closeUnderlyingFileChannel;
    }

    @Override
    public long getPosition() {
        return positionCounter.get();
    }

    @Override
    public void setPosition(long newPosition) {
        if (closed)
            throw new IllegalStateException("Reader is closed.");
        if (!hasPendingOperation.compareAndSet(false, true))
            throw new IllegalStateException("Can't set position during active operation.");
        positionCounter.set(newPosition);
        hasPendingOperation.set(false);
    }

    private void startOperation() {
        if (closed)
            throw new IllegalStateException("Reader is closed.");
        if (!hasPendingOperation.compareAndSet(false, true))
            throw new IllegalStateException("Can't set position during active operation.");
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        startOperation();
        fileChannel.read(dst, positionCounter.get(), attachment, new CompletionHandlerAdapter<>(handler));
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        FutureCompletable future = new FutureCompletable();
        read(dst, null, future.completionHandler());
        return future;
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        startOperation();
        fileChannel.write(src, positionCounter.get(), attachment, new CompletionHandlerAdapter<>(handler));
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        FutureCompletable future = new FutureCompletable();
        write(src, null, future.completionHandler());
        return future;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (closeUnderlyingFileChannel)
            fileChannel.close();
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    /**
     * Creates AsynchronousFileChannelAdapter backed by the same AsynchronousFileChannel with separate position counter.
     * Closing child adapter will not close underlying fileChannel on it's close.
     */
    public AsynchronousFileChannelAdapter createChildAdapter() {
        return createChildAdapter(-1);
    }

    /**
     * Creates AsynchronousFileChannelAdapter backed by the same AsynchronousFileChannel with separate position counter.
     * Closing child adapter will not close underlying fileChannel on it's close.
     *
     * @param startPosition start new channel adapter from this file position; -1 to inherit current position from this reader
     */
    public AsynchronousFileChannelAdapter createChildAdapter(long startPosition) {
        if (closed)
            throw new IllegalStateException("Reader is closed.");
        if (hasPendingOperation.get())
            throw new IllegalStateException("Can't set position during active operation.");
        if (startPosition == -1)
            startPosition = positionCounter.get();
        return new AsynchronousFileChannelAdapter(fileChannel, startPosition, false);
    }

    private static final Callable NOOP = () -> null;

    private final class CompletionHandlerAdapter<A> implements CompletionHandler<Integer, A> {
        final CompletionHandler<Integer, ? super A> innerHandler;

        CompletionHandlerAdapter(CompletionHandler<Integer, ? super A> innerHandler) {
            this.innerHandler = innerHandler;
        }

        @Override
        public void completed(Integer result, A attachment) {
            hasPendingOperation.set(false);
            positionCounter.addAndGet(result);
            innerHandler.completed(result, attachment);
        }

        @Override
        public void failed(Throwable exc, A attachment) {
            hasPendingOperation.set(false);
            innerHandler.failed(exc, attachment);
        }
    }

    private static final class FutureCompletable extends FutureTask<Integer> {
        FutureCompletable() {
            super(NOOP);
        }

        <A> CompletionHandler<Integer, A> completionHandler() {
            return new CompletionHandler<Integer, A>() {
                @Override
                public void completed(Integer result, A attachment) {
                    FutureCompletable.super.set(result);
                }

                @Override
                public void failed(Throwable exc, A attachment) {
                    FutureCompletable.super.setException(exc);
                }
            };
        }
    }
}
