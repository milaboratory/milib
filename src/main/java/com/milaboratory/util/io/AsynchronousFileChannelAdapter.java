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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Partial adapter between AsynchronousFileChannel and AsynchronousByteChannel.
 */
public final class AsynchronousFileChannelAdapter implements AsynchronousByteChannel, HasPosition {
    final AsynchronousFileChannel fileChannel;
    final AtomicLong positionCounter;

    public AsynchronousFileChannelAdapter(AsynchronousFileChannel fileChannel, long position) {
        this.fileChannel = fileChannel;
        this.positionCounter = new AtomicLong(position);
    }

    @Override
    public long getPosition() {
        return positionCounter.get();
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        fileChannel.read(dst, positionCounter.get(), attachment, new CompletionHandler<Integer, A>() {
            @Override
            public void completed(Integer result, A attachment) {
                positionCounter.addAndGet(result);
                handler.completed(result, attachment);
            }

            @Override
            public void failed(Throwable exc, A attachment) {
                handler.failed(exc, attachment);
            }
        });
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        FutureCompletable<Integer> future = new FutureCompletable<>();
        read(dst, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.set(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.setException(exc);
            }
        });
        return future;
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        long position = positionCounter.getAndAdd(src.limit());
        fileChannel.write(src, position, attachment, handler);
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        FutureCompletable<Integer> future = new FutureCompletable<>();
        read(src, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.set(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.setException(exc);
            }
        });
        return future;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    @Override
    public boolean isOpen() {
        return fileChannel.isOpen();
    }

    private static final Callable NOOP = () -> null;

    private static final class FutureCompletable<T> extends FutureTask<T> {
        public FutureCompletable() {
            super(NOOP);
        }

        @Override
        public void set(T aVoid) {
            super.set(aVoid);
        }

        @Override
        public void setException(Throwable t) {
            super.setException(t);
        }
    }
}
