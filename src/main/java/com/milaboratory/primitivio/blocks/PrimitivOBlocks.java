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
import com.milaboratory.util.io.ByteArrayDataOutput;
import com.milaboratory.util.io.IOUtil;
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

import static com.milaboratory.util.io.IOUtil.writeIntBE;


/**
 * Block:
 *
 * Header (17 bytes total):
 * [ 1 byte : bit0 = (0 = last block ; 1 = data block);
 * bit1 = (0 = raw ; 1 = compressed);
 * bit2 = (0 = data block ; 1 = special block) ]
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
    private static final byte[] LAST_HEADER = new byte[BLOCK_HEADER_SIZE];

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
     * @param executor
     * @param concurrency
     * @param outputState
     * @param blockSize   number of objects in a block
     * @param compressor
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
     * Block serialization, CPU intensive part
     */
    private ByteBuffer serializeBlock(List<O> content) {
        // // Assert
        // assert content.size() != 0;
        // boolean assertOn = false;
        // assert assertOn = true;
        // //noinspection ConstantConditions
        // if (!assertOn && content.size() == 0)
        //     System.err.println("Writing empty block in AlignmentsIO.");

        // buffers.ensureRawBufferSize(alignments.size() * AVERAGE_ALIGNMENT_SIZE);

        // TODO init with average object size
        ByteArrayDataOutput dataOutput = new ByteArrayDataOutput();

        // Stats {
        long start = System.nanoTime();
        // }

        PrimitivO output = outputState.createPrimitivO(dataOutput);

        // Writing alignments to memory buffer
        for (O obj : content)
            output.writeObject(obj);

        // Stats {
        serializationNanos.addAndGet(System.nanoTime() - start);
        uncompressedBytes.addAndGet(dataOutput.size());
        start = System.nanoTime();
        // }

        int checksum = xxHash32.hash(dataOutput.getBuffer(), 0, dataOutput.size(), HASH_SEED);

        // Stats {
        checksumNanos.addAndGet(System.nanoTime() - start);
        start = System.nanoTime();
        // }

        byte[] block = new byte[BLOCK_HEADER_SIZE + compressor.maxCompressedLength(dataOutput.size())];
        int compressedLength = compressor.compress(dataOutput.getBuffer(), 0, dataOutput.size(),
                block, BLOCK_HEADER_SIZE);

        compressionNanos.addAndGet(System.nanoTime() - start);

        // Header field 1
        writeIntBE(content.size(), block, 1);

        int blockSize;

        if (compressedLength > dataOutput.size()) {
            // Compression increased data size -> writing uncompressed block
            System.arraycopy(dataOutput.getBuffer(), 0, block, BLOCK_HEADER_SIZE, dataOutput.size());
            block[0] = 0x1; // bit0 = 1, bit1 = 0
            writeIntBE(dataOutput.size(), block, 5);
            writeIntBE(dataOutput.size(), block, 9);
            blockSize = BLOCK_HEADER_SIZE + dataOutput.size();
        } else {
            block[0] = 0x3; // bit0 = 1, bit1 = 1
            writeIntBE(dataOutput.size(), block, 5);
            writeIntBE(compressedLength, block, 9);
            blockSize = BLOCK_HEADER_SIZE + compressedLength;
        }

        // Writing checksum
        writeIntBE(checksum, block, 13);

        objectCount.addAndGet(content.size());
        outputSize.addAndGet(blockSize);
        blockCount.incrementAndGet();

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
        return newWriter(IOUtil.toAsynchronousByteChannel(channel, position), closeUnderlyingChannel);
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

        public synchronized void write(O obj) {
            buffer.add(obj);
            if (blockIsFull(buffer.size())) {
                writeBlock(buffer);
                buffer = new ArrayList<>();
            }
        }

        public synchronized void writeBlock(final List<O> content) {
            // Checking for errors before and after throttling
            checkException();

            // Concurrency limits the whole block lifecycle (serialization + IO)
            // so operations will be throttled both on IO and CPU
            concurrencyLimiter.acquireUninterruptibly();

            // Checking for errors before and after throttling
            checkException();

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

                    // Adding a callback after the last IO operation to issue write operation of the current block
                    previousLatch.setCallback(
                            () -> {
                                long ioBegin = System.nanoTime();
                                channel.write(block, null,
                                        new CHAbstract() {
                                            @Override
                                            public void completed(Integer result, Object attachment) {
                                                ioDelayNanos.addAndGet(System.nanoTime() - ioBegin);

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
                    exception = e;
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void close() {
            try {
                closed = true;

                if (!buffer.isEmpty())
                    // Writing leftovers
                    writeBlock(buffer);
                else
                    // Anyway check for errors before proceed
                    checkException();

                // Writing final block
                LambdaLatch previousLatch = currentWriteLatch;
                LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();
                outputSize.addAndGet(BLOCK_HEADER_SIZE);
                previousLatch.setCallback(() -> channel.write(ByteBuffer.wrap(LAST_HEADER), null,
                        new CHAbstract() {
                            @Override
                            public void completed(Integer result, Object attachment) {
                                nextLatch.open();
                            }
                        }));

                // Waiting EOF header to be flushed to the stream
                nextLatch.await();

                if (closeUnderlyingChannel)
                    channel.close();

            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public PrimitivOBlocksStats getStats() {
        return new PrimitivOBlocksStats(System.nanoTime() - initializationTimestamp,
                totalSerializationNanos.get(), serializationNanos.get(), checksumNanos.get(),
                compressionNanos.get(), ioDelayNanos.get(), uncompressedBytes.get(), concurrencyOverhead.get(),
                outputSize.get(), blockCount.get(), objectCount.get(), concurrency);
    }

}
