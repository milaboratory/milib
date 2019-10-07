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

import cc.redberry.pipe.InputPort;
import com.milaboratory.primitivio.PrimitivO;
import com.milaboratory.primitivio.PrimitivOState;
import com.milaboratory.util.LambdaLatch;
import com.milaboratory.util.io.AsynchronousFileChannelAdapter;
import com.milaboratory.util.io.ByteArrayDataOutput;
import com.milaboratory.util.io.HasPosition;
import net.jpountz.lz4.LZ4Compressor;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Block:
 *
 * Header (17 bytes total):
 * [ 1 byte : bit0 = (0 = last block ; 1 = data block); bit1 = (0 = raw ; 1 = compressed); bit2 = (0 = data block ; 1 = special block) ]
 * ( [ 4 bytes : int : number of objects ]
 * [ 4 bytes : int : rawDataSize ]
 * [ 4 bytes : int : compressedDataSize / blockSize ]
 * [ 4 bytes : int : checksum for the raw data ] )
 * |
 * ( [ 16 bytes : special block ] )
 *
 * Data:
 * [ dataSize bytes ] (compressed, if bit1 of header is 1; uncompressed, if bit1 is 0; no bytes for special blocks )
 */
public final class PrimitivOBlocks<O> extends PrimitivIOBlocksAbstract {
    /**
     * LZ4 compressor to compress data blocks
     */
    private final LZ4Compressor compressor;

    /**
     * PrimitivO stream state
     */
    private final PrimitivOState outputState;

    /**
     * Limits the number of active serializing blocks
     */
    private final Semaphore concurrencyLimiter;

    /**
     * Block size
     */
    private final int blockSize;

    // Statistics
    // throttlingNanos = new AtomicLong(),
    private final AtomicLong
            totalSerializationNanos = new AtomicLong(),
            serializationNanos = new AtomicLong(),
            checksumNanos = new AtomicLong(),
            compressionNanos = new AtomicLong(),
            ioDelayNanos = new AtomicLong(),
            uncompressedBytes = new AtomicLong(),
            outputSize = new AtomicLong(),
            concurrencyOverhead = new AtomicLong(),
            blockCount = new AtomicLong(),
            objectCount = new AtomicLong();

    private long initializationTimestamp = System.nanoTime();

    /**
     * @param executor    executor to execute serialization process in
     *                    (the same executor service as used in target AsynchronousByteChannels is recommended)
     * @param concurrency maximal number of concurrent serializations
     * @param outputState knownReferences and objects, etc.
     * @param blockSize   number of objects in a block
     * @param compressor  block compressor
     */
    public PrimitivOBlocks(ExecutorService executor, int concurrency,
                           PrimitivOState outputState, int blockSize, LZ4Compressor compressor) {
        super(executor, concurrency);
        this.compressor = compressor;
        this.outputState = outputState;
        this.blockSize = blockSize;
        this.concurrencyLimiter = new Semaphore(concurrency);
    }

    public void resetStats() {
        initializationTimestamp = System.nanoTime();
        totalSerializationNanos.set(0);
        serializationNanos.set(0);
        checksumNanos.set(0);
        compressionNanos.set(0);
        ioDelayNanos.set(0);
        uncompressedBytes.set(0);
        outputSize.set(0);
        concurrencyOverhead.set(0);
        blockCount.set(0);
        objectCount.set(0);
    }

    private boolean blockIsFull(int numberOfObjects) {
        return numberOfObjects >= blockSize;
    }

    /**
     * Block serialization, CPU intensive part.
     *
     * Returns header + data.
     *
     * Executed from {@link Writer} class.
     */
    private ByteBuffer serializeBlock(List<O> content) {
        // Stats {
        ongoingSerdes.incrementAndGet();
        // }

        ByteArrayDataOutput uncompressedOutput = blockCount.get() > 0
                ? new ByteArrayDataOutput((int) (uncompressedBytes.get() / blockCount.get()))
                : new ByteArrayDataOutput();

        // Stats {
        long start = System.nanoTime();
        // }

        PrimitivO output = outputState.createPrimitivO(uncompressedOutput);

        // Writing objects to memory buffer
        for (O obj : content)
            output.writeObject(obj);

        // Creating header
        PrimitivIOBlockHeader header = PrimitivIOBlockHeader.dataBlockHeader();

        // Stats {
        serializationNanos.addAndGet(System.nanoTime() - start);
        uncompressedBytes.addAndGet(uncompressedOutput.size());
        start = System.nanoTime();
        // }

        header.setChecksum(xxHash32.hash(uncompressedOutput.getBuffer(), 0, uncompressedOutput.size(), HASH_SEED));

        // Stats {
        checksumNanos.addAndGet(System.nanoTime() - start);
        start = System.nanoTime();
        // }

        byte[] block = new byte[BLOCK_HEADER_SIZE + compressor.maxCompressedLength(uncompressedOutput.size())];
        int compressedLength = compressor.compress(uncompressedOutput.getBuffer(), 0, uncompressedOutput.size(),
                block, BLOCK_HEADER_SIZE);

        compressionNanos.addAndGet(System.nanoTime() - start);

        // Setting header fields
        header
                .setNumberOfObjects(content.size())
                .setUncompressedDataSize(uncompressedOutput.size());

        int blockSize;

        if (compressedLength > uncompressedOutput.size()) {
            // Compression increased data size -> writing uncompressed block
            System.arraycopy(uncompressedOutput.getBuffer(), 0, block, BLOCK_HEADER_SIZE, uncompressedOutput.size());
            header.setDataSize(uncompressedOutput.size());
            // Saving actual block size
            blockSize = BLOCK_HEADER_SIZE + uncompressedOutput.size();
        } else {
            header.setCompressed().setDataSize(compressedLength);
            // Saving actual block size
            blockSize = BLOCK_HEADER_SIZE + compressedLength;
        }

        header.writeTo(block, 0);

        objectCount.addAndGet(content.size());
        outputSize.addAndGet(blockSize);
        blockCount.incrementAndGet();

        // Stats {
        ongoingSerdes.decrementAndGet();
        // }

        return ByteBuffer.wrap(block, 0, blockSize);
    }

    /**
     * Helper method to create async channel for writing with this object's execution service
     */
    public AsynchronousFileChannel createAsyncChannel(Path path, OpenOption... additionalOptions) throws IOException {
        return createAsyncChannel(path, additionalOptions, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    public Writer newWriter(Path channel) throws IOException {
        return newWriter(createAsyncChannel(channel), 0, true);
    }

    public Writer newWriter(AsynchronousFileChannel channel, long position) {
        return newWriter(channel, position, false);
    }

    public Writer newWriter(AsynchronousFileChannel channel, long position, boolean closeUnderlyingChannel) {
        return newWriter(new AsynchronousFileChannelAdapter(channel, position), closeUnderlyingChannel);
    }

    public Writer newWriter(AsynchronousByteChannel channel, boolean closeUnderlyingChannel) {
        return new Writer(channel, closeUnderlyingChannel);
    }

    public final class Writer implements InputPort<O>, AutoCloseable, Closeable {
        final AsynchronousByteChannel channel;
        final boolean closeUnderlyingChannel;

        // Accessed from synchronized method, initially opened
        LambdaLatch currentWriteLatch = new LambdaLatch(true);
        List<O> buffer = new ArrayList<>();
        boolean closed = false;

        Writer(AsynchronousByteChannel channel, boolean closeUnderlyingChannel) {
            this.channel = channel;
            this.closeUnderlyingChannel = closeUnderlyingChannel;
        }

        public PrimitivOBlocks<O> getParent() {
            return PrimitivOBlocks.this;
        }

        @Override
        public void put(O object) {
            if (object != null)
                write(object);
            else
                close();
        }

        /**
         * Flush internal object buffer. Does not wait for buffer to be serialized and flushed to underlying channel.
         */
        public synchronized void flush() {
            if (!buffer.isEmpty()) {
                writeBlock(buffer);
                buffer = new ArrayList<>();
            }
        }

        public synchronized void write(O obj) {
            buffer.add(obj);
            if (blockIsFull(buffer.size())) {
                flush();
            }
        }

        /**
         * Wait for all async operations to finish
         */
        public synchronized void sync() {
            try {
                currentWriteLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }

        /**
         * Return current position if underlying channel supports position
         */
        public synchronized long getPosition() {
            if (channel instanceof HasPosition) {
                sync();
                return ((HasPosition) channel).getPosition();
            } else
                throw new IllegalStateException("Underlying channel does not support information retrieval.");
        }

        public synchronized void writeHeader(PrimitivIOBlockHeader header) {
            if (!buffer.isEmpty())
                throw new IllegalStateException("Buffer is not empty. Invoke flush() before writeHeader(...).");

            // Checking for errors
            checkException();

            // Creating latches for IO operations ordering
            LambdaLatch previousLatch = currentWriteLatch;
            LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();

            // Header bytes
            ByteBuffer block = header.asByteBuffer();
            int size = block.limit();

            // Tracking output size
            outputSize.addAndGet(BLOCK_HEADER_SIZE);

            pendingOps.incrementAndGet();

            // Adding a callback after the last IO operation to issue write operation of the current block
            previousLatch.setCallback(
                    () -> {
                        pendingOps.decrementAndGet();
                        ongoingIOOps.incrementAndGet();
                        long ioBegin = System.nanoTime();
                        channel.write(block, null,
                                new CHAbstract() {
                                    @Override
                                    public void completed(Integer result, Object attachment) {
                                        ioDelayNanos.addAndGet(System.nanoTime() - ioBegin);
                                        ongoingIOOps.decrementAndGet();

                                        // Assert
                                        if (result != size) {
                                            exception = new RuntimeException("Wrong block size.");
                                            exception.printStackTrace();
                                            // Releasing a permit for the next operation to detect the error
                                            concurrencyLimiter.release();
                                            return;
                                        }

                                        // Opening latch for the next IO operation
                                        nextLatch.open();
                                    }
                                });
                    });
        }

        public synchronized void writeBlock(final List<O> content) {
            // Checking for errors before and after throttling
            checkException();

            // Concurrency limits the whole block lifecycle (serialization + IO)
            // so operations will be throttled both on IO and CPU
            //
            // This operation will block caller thread (this is the one out of two blocking operations for the whole
            // PrimitivIOBlocks suite)
            concurrencyLimiter.acquireUninterruptibly();

            // Checking for errors before and after throttling
            checkException();

            // Creating latches for IO operations ordering
            LambdaLatch previousLatch = currentWriteLatch;
            LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();

            long submittedTimestamp = System.nanoTime();
            executor.submit(() -> {
                concurrencyOverhead.addAndGet(System.nanoTime() - submittedTimestamp);
                try {
                    long start = System.nanoTime();

                    // CPU intensive operation, no IO
                    ByteBuffer block = serializeBlock(content);

                    totalSerializationNanos.addAndGet(System.nanoTime() - start);

                    int blockSize = block.limit();

                    pendingOps.incrementAndGet();

                    // Adding a callback after the last IO operation to issue write operation of the current block
                    previousLatch.setCallback(
                            () -> {
                                pendingOps.decrementAndGet();
                                ongoingIOOps.incrementAndGet();
                                long ioBegin = System.nanoTime();
                                channel.write(block, null,
                                        new CHAbstract() {
                                            @Override
                                            public void completed(Integer result, Object attachment) {
                                                ioDelayNanos.addAndGet(System.nanoTime() - ioBegin);
                                                ongoingIOOps.decrementAndGet();

                                                // Assert
                                                if (result != blockSize) {
                                                    exception = new RuntimeException("Wrong block size.");
                                                    exception.printStackTrace();
                                                    // Releasing a permit for the next operation to detect the error
                                                    concurrencyLimiter.release();
                                                    return;
                                                }

                                                // Releasing a permit for the next serialization (CPU intensive) operation
                                                concurrencyLimiter.release();

                                                // Opening latch for the next IO operation
                                                nextLatch.open();
                                            }
                                        });
                            });
                } catch (Exception e) {
                    // TODO _ex method like in IBlocks ?
                    exception = e;
                    // Releasing a permit for the next operation to detect the error
                    concurrencyLimiter.release();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void close() {
            try {
                if (closed)
                    return;

                closed = true;

                if (!buffer.isEmpty())
                    // Writing leftovers
                    flush();
                else
                    // Anyway check for errors before proceed
                    checkException();

                // Writing final header
                writeHeader(PrimitivIOBlockHeader.lastHeader());

                // Waiting EOF header to be flushed to the stream
                sync();

                // Just in case
                checkException();

                if (closeUnderlyingChannel)
                    channel.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public PrimitivOBlocksStats getStats() {
        return new PrimitivOBlocksStats(System.nanoTime() - initializationTimestamp,
                totalSerializationNanos.get(), serializationNanos.get(), checksumNanos.get(),
                compressionNanos.get(), ioDelayNanos.get(), uncompressedBytes.get(), concurrencyOverhead.get(),
                outputSize.get(), blockCount.get(), objectCount.get(),
                ongoingSerdes.get(), ongoingIOOps.get(), pendingOps.get(),
                concurrency);
    }

}
