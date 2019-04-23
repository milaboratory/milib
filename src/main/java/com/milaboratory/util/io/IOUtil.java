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

public final class IOUtil {
    private IOUtil() {
    }

    public static void writeIntBE(int val, byte[] buffer, int offset) {
        buffer[offset] = (byte) (val >>> 24);
        buffer[offset + 1] = (byte) (val >>> 16);
        buffer[offset + 2] = (byte) (val >>> 8);
        buffer[offset + 3] = (byte) val;
    }

    public static void writeLongBE(long val, byte[] buffer, int offset) {
        buffer[offset] = (byte) (val >>> 56);
        buffer[offset + 1] = (byte) (val >>> 48);
        buffer[offset + 2] = (byte) (val >>> 40);
        buffer[offset + 3] = (byte) (val >>> 32);
        buffer[offset + 4] = (byte) (val >>> 24);
        buffer[offset + 5] = (byte) (val >>> 16);
        buffer[offset + 6] = (byte) (val >>> 8);
        buffer[offset + 7] = (byte) val;
    }

    public static int readIntBE(byte[] buffer, int offset) {
        int value = 0;
        for (int i = 0; i < 4; ++i) {
            value <<= 8;
            value |= 0xFF & buffer[offset + i];
        }
        return value;
    }

    public static int readIntBE(ByteBuffer buffer) {
        int value = 0;
        for (int i = 0; i < 4; ++i) {
            value <<= 8;
            value |= 0xFF & buffer.get();
        }
        return value;
    }

    public static long readLongBE(byte[] buffer, int offset) {
        long value = 0;
        for (int i = 0; i < 8; ++i) {
            value <<= 8;
            value |= 0xFF & buffer[offset++];
        }
        return value;
    }

    public static long readLongBE(ByteBuffer buffer) {
        long value = 0;
        for (int i = 0; i < 8; ++i) {
            value <<= 8;
            value |= 0xFF & buffer.get();
        }
        return value;
    }

    /**
     * Creates partial adapter between AsynchronousFileChannel and AsynchronousByteChannel.
     */
    public static AsynchronousByteChannel toAsynchronousByteChannel(AsynchronousFileChannel fileChannel, long position) {
        AtomicLong positionCounter = new AtomicLong(position);
        return new AsynchronousByteChannel() {
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
        };
    }

    static final Callable NOOP = () -> null;

    static final class FutureCompletable<T> extends FutureTask<T> {
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
