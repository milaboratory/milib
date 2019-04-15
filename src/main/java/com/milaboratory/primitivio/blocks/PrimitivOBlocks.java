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
import com.milaboratory.util.LambdaLatch;
import com.milaboratory.util.TimeUtils;
import com.milaboratory.util.io.ByteArrayDataOutput;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
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
 * [ 4 bytes : int : compressedDataSize ]
 * [ 4 bytes : int : checksum for the raw data ] )
 * |
 * ( [ 16 bytes : special block ] )
 *
 * Data:
 * [ dataSize bytes ] (compressed, if bit1 of header is 1; uncompressed, if bit1 is 0; no bytes for special blocks )
 */
public final class PrimitivOBlocks<O> {
    static final int HASH_SEED = 0xD5D20F71;
    static final int BLOCK_HEADER_SIZE = 17;

    private static final byte[] LAST_HEADER = new byte[BLOCK_HEADER_SIZE];

    /**
     * Executor for the deserialization routines
     */
    final ExecutorService executor;

    /**
     * LZ4 compressor to compress data blocks
     */
    private final LZ4Compressor compressor;

    /**
     * LZ4 hash function
     */
    private final XXHash32 xxHash32 = XXHashFactory.fastestJavaInstance().hash32();

    /**
     * PrimitivO stream state
     */
    private final PrimitivOState outputState;

    /**
     * Limits the number of active serializing blocks
     */
    private final Semaphore concurrencyLimiter;

    /**
     * Signal the error in one of the asynchronous actions
     */
    private volatile Throwable exception = null;

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
            blockCount = new AtomicLong();

    /**
     * @param executor
     * @param compressor
     * @param outputState
     * @param concurrency 0 for unlimited concurrency
     */
    public PrimitivOBlocks(ExecutorService executor, LZ4Compressor compressor, PrimitivOState outputState, int concurrency) {
        this.executor = executor;
        this.compressor = compressor;
        this.outputState = outputState;
        this.concurrencyLimiter = concurrency >= 1 ? new Semaphore(concurrency) : null;
    }

    private boolean blockIsFull(int numberOfObjects) {
        // TODO add logic here....
        return numberOfObjects > 8000;
    }

    private void checkException() {
        if (exception != null)
            throw new RuntimeException(exception);
    }

    /**
     * Write CPU intensive part
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
            block[0] = 0x1; // bit0 = 1, bit0 = 0
            writeIntBE(dataOutput.size(), block, 5);
            writeIntBE(dataOutput.size(), block, 9);
            blockSize = BLOCK_HEADER_SIZE + dataOutput.size();
        } else {
            block[0] = 0x3; // bit0 = 1, bit0 = 1
            writeIntBE(dataOutput.size(), block, 5);
            writeIntBE(compressedLength, block, 9);
            blockSize = BLOCK_HEADER_SIZE + compressedLength;
        }

        // Writing checksum
        writeIntBE(checksum, block, 13);

        outputSize.addAndGet(blockSize);
        blockCount.incrementAndGet();

        return ByteBuffer.wrap(block, 0, blockSize);
    }

    public Writer newWriter(AsynchronousFileChannel channel, long position) {
        return new Writer(channel, position);
    }

    public final class Writer implements AutoCloseable, Closeable {
        final AsynchronousFileChannel channel;

        // Accessed from synchronized method
        LambdaLatch currentWriteLatch;
        List<O> buffer = new ArrayList<>();
        boolean closed = false;

        // Accessed from async operations
        volatile long position;

        Writer(AsynchronousFileChannel channel, long position) {
            this.channel = channel;
            this.position = position;

            // Creating opened latch
            this.currentWriteLatch = new LambdaLatch();
            this.currentWriteLatch.open();
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
            if (concurrencyLimiter != null)
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
                                channel.write(block, position, null,
                                        new CHAbstract() {
                                            @Override
                                            public void completed(Integer result, Object attachment) {
                                                ioDelayNanos.addAndGet(System.nanoTime() - ioBegin);

                                                // Assert
                                                if (result != blockSize) {
                                                    exception = new RuntimeException("Wrong block size.");
                                                    exception.printStackTrace();
                                                    // Releasing a permit for the next operation to detect the error
                                                    if (concurrencyLimiter != null)
                                                        concurrencyLimiter.release();
                                                    return;
                                                }

                                                // Because write operations are serialized using latches,
                                                // there is no concurrent access to the position variable here
                                                // noinspection NonAtomicOperationOnVolatileField
                                                position += blockSize;

                                                // Releasing a permit for the next serialization (CPU intensive) operation
                                                if (concurrencyLimiter != null)
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
        public synchronized void close() throws IOException {
            try {
                closed = true;
                checkException();
                LambdaLatch previousLatch = currentWriteLatch;
                LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();

                outputSize.addAndGet(BLOCK_HEADER_SIZE);

                previousLatch.setCallback(() -> channel.write(ByteBuffer.wrap(LAST_HEADER),
                        position, null,
                        new CHAbstract() {
                            @Override
                            public void completed(Integer result, Object attachment) {
                                position += BLOCK_HEADER_SIZE;
                                nextLatch.open();
                            }
                        }));
                nextLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getStatsString() {
        return "totalSerializationNanos: " + TimeUtils.nanoTimeToString(totalSerializationNanos.get()) + "\n" +
                "serializationNanos: " + TimeUtils.nanoTimeToString(serializationNanos.get()) + "\n" +
                "checksumNanos: " + TimeUtils.nanoTimeToString(checksumNanos.get()) + "\n" +
                "compressionNanos: " + TimeUtils.nanoTimeToString(compressionNanos.get()) + "\n" +
                "ioDelayNanos: " + TimeUtils.nanoTimeToString(ioDelayNanos.get()) + "\n" +
                "concurrencyOverhead: " + TimeUtils.nanoTimeToString(concurrencyOverhead.get()) + "\n" +
                "uncompressedBytes: " + uncompressedBytes.get() + "\n" +
                "outputSize: " + outputSize.get() + "\n" +
                "blockCount: " + blockCount.get();

    }

    private abstract class CHAbstract implements CompletionHandler<Integer, Object> {
        @Override
        public void failed(Throwable exc, Object attachment) {
            exc.printStackTrace();
            exception = exc;
        }
    }
}
