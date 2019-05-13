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

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import com.milaboratory.primitivio.PrimitivI;
import com.milaboratory.primitivio.PrimitivIState;
import com.milaboratory.util.LambdaLatch;
import com.milaboratory.util.LambdaSemaphore;
import com.milaboratory.util.io.AsynchronousFileChannelAdapter;
import com.milaboratory.util.io.ByteBufferDataInputAdapter;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.milaboratory.util.io.IOUtil.readIntBE;

public final class PrimitivIBlocks<O> extends PrimitivIOBlocksAbstract {
    /**
     * Class of target objects
     */
    private final Class<O> clazz;

    /**
     * LZ4 compressor to compress data blocks
     */
    private final LZ4FastDecompressor decompressor;

    /**
     * PrimitivI stream state
     */
    private final PrimitivIState inputState;

    /**
     * Concurrency limiting object, to keep number of concurrent IO and CPU intensive tasks within a certain limit.
     *
     * Throttled tasks are queued for execution when execution permits become available.
     */
    private final LambdaSemaphore concurrencyLimiter;

    // Stats
    private final AtomicLong
            totalDeserializationNanos = new AtomicLong(),
            deserializationNanos = new AtomicLong(),
            checksumNanos = new AtomicLong(),
            decompressionNanos = new AtomicLong(),
            ioDelayNanos = new AtomicLong(),
            uncompressedBytes = new AtomicLong(),
            inputSize = new AtomicLong(),
            blockCount = new AtomicLong(),
            objectCount = new AtomicLong();

    private long initializationTimestamp = System.nanoTime();

    /**
     * @param executor     executor to execute serialization process in
     *                     (the same executor service as used in target AsynchronousByteChannels is recommended)
     * @param concurrency  maximal number of concurrent deserializations, actual concurrency level is also limited by
     *                     readAheadBlocks parameter (effective concurrency will be ~min(readAheadBlocks, concurrency))
     *                     and IO speed
     * @param clazz        class to deserialize
     * @param decompressor block decompressor
     * @param inputState   stream state
     */
    public PrimitivIBlocks(ExecutorService executor, int concurrency, Class<O> clazz, LZ4FastDecompressor decompressor, PrimitivIState inputState) {
        super(executor, concurrency);
        this.clazz = clazz;
        this.decompressor = decompressor;
        this.inputState = inputState;
        this.concurrencyLimiter = new LambdaSemaphore(concurrency);
    }

    public void resetStats() {
        initializationTimestamp = System.nanoTime();
        totalDeserializationNanos.set(0);
        deserializationNanos.set(0);
        checksumNanos.set(0);
        decompressionNanos.set(0);
        ioDelayNanos.set(0);
        uncompressedBytes.set(0);
        inputSize.set(0);
        blockCount.set(0);
        objectCount.set(0);
    }

    /**
     * Block deserialization, CPU intensive part
     */
    private List<O> deserializeBlock(byte[] header, byte[] blockAndNextHeader) {
        // Reading header
        int numberOfObjects = readIntBE(header, 1);
        int checksum = readIntBE(header, 13);
        int blockLength = blockAndNextHeader.length - BLOCK_HEADER_SIZE;
        assert blockLength == readIntBE(header, 9);

        inputSize.addAndGet(blockAndNextHeader.length);

        // Stats {
        long start = System.nanoTime();
        // }

        byte[] data;
        int dataLen;
        if ((header[0] & 0x2) != 0) { // Compressed block
            int decompressedLength = readIntBE(header, 5);
            data = new byte[decompressedLength];
            // TODO correct method ???
            decompressor.decompress(blockAndNextHeader, data);
            dataLen = decompressedLength;
        } else {// Uncompressed block
            data = blockAndNextHeader;
            dataLen = blockLength;
        }

        // Stats {
        decompressionNanos.addAndGet(System.nanoTime() - start);
        uncompressedBytes.addAndGet(dataLen);
        start = System.nanoTime();
        // }

        int actualChecksum = xxHash32.hash(data, 0, dataLen, HASH_SEED);

        // Stats {
        checksumNanos.addAndGet(System.nanoTime() - start);
        start = System.nanoTime();
        // }

        if (actualChecksum != checksum)
            throw new RuntimeException("Checksum mismatch. Malformed file.");

        ByteBufferDataInputAdapter dataInput = new ByteBufferDataInputAdapter(ByteBuffer.wrap(data, 0, dataLen));
        PrimitivI primitivI = inputState.createPrimitivI(dataInput);

        // Deserialization
        ArrayList<O> content = new ArrayList<>(numberOfObjects);
        for (int i = 0; i < numberOfObjects; i++)
            content.add(primitivI.readObject(clazz));

        // Stats {
        deserializationNanos.addAndGet(System.nanoTime() - start);
        // }

        blockCount.incrementAndGet();
        objectCount.addAndGet(content.size());

        return content;
    }

    public PrimitivIBlocksStats getStats() {
        return new PrimitivIBlocksStats(
                System.nanoTime() - initializationTimestamp,
                totalDeserializationNanos.get(),
                deserializationNanos.get(),
                checksumNanos.get(),
                decompressionNanos.get(),
                ioDelayNanos.get(),
                uncompressedBytes.get(),
                inputSize.get(),
                blockCount.get(),
                objectCount.get(),
                concurrency);
    }

    /**
     * Helper method to create async channel for reading with this object's execution service
     */
    public AsynchronousFileChannel createAsyncChannel(Path path, OpenOption... additionalOptions) throws IOException {
        return createAsyncChannel(path, additionalOptions, StandardOpenOption.READ);
    }

    public Reader newReader(Path channel, int readAheadBlocks) throws IOException {
        return newReader(createAsyncChannel(channel), readAheadBlocks, 0, true);
    }

    public Reader newReader(AsynchronousFileChannel channel, int readAheadBlocks, long position) {
        return newReader(channel, readAheadBlocks, position, false);
    }

    public Reader newReader(AsynchronousFileChannel channel, int readAheadBlocks, long position, boolean closeUnderlyingChannel) {
        return newReader(new AsynchronousFileChannelAdapter(channel, position), readAheadBlocks, closeUnderlyingChannel);
    }

    public Reader newReader(AsynchronousByteChannel channel, int readAheadBlocks, boolean closeUnderlyingChannel) {
        return new Reader(channel, readAheadBlocks, closeUnderlyingChannel);
    }

    public final class Reader implements OutputPortCloseable<O> {
        final AsynchronousByteChannel channel;
        final int readAheadBlocks;
        final boolean closeUnderlyingChannel;

        // Accessed from synchronized method, initially opened
        LambdaLatch currentIOLatch = new LambdaLatch(true);

        /**
         * End of stream was detected during previous IO operation
         */
        volatile boolean eof = false;

        /**
         * Stores current header
         */
        volatile byte[] nextHeader;

        /**
         * Blocks being red ahead
         */
        final ArrayDeque<Block<O>> blocks = new ArrayDeque<>();
        OutputPort<O> currentBlock = null;
        volatile boolean closed = false;

        public Reader(AsynchronousByteChannel channel, int readAheadBlocks, boolean closeUnderlyingChannel) {
            this.channel = channel;
            this.readAheadBlocks = readAheadBlocks;
            this.closeUnderlyingChannel = closeUnderlyingChannel;
            readHeader();
            readBlocksIfNeeded();
        }

        public PrimitivIBlocks<O> getParent() {
            return PrimitivIBlocks.this;
        }

        private synchronized void readHeader() {
            checkException();

            LambdaLatch previousLatch = currentIOLatch;
            LambdaLatch nextLatch = currentIOLatch = new LambdaLatch();

            previousLatch.setCallback(() -> {
                if (closed)
                    return;

                byte[] header = new byte[BLOCK_HEADER_SIZE];
                ByteBuffer buffer = ByteBuffer.wrap(header);
                long ioStart = System.nanoTime();
                channel.read(buffer, null, new CHAbstract() {
                    @Override
                    public void completed(Integer result, Object attachment) {
                        // Recording time spent waiting for io operation completion
                        ioDelayNanos.addAndGet(System.nanoTime() - ioStart);

                        if (closed)
                            return;

                        // Assert
                        if (result != BLOCK_HEADER_SIZE) {
                            exception = new RuntimeException("Premature EOF.");
                            exception.printStackTrace();

                            // TODO ?????????
                            // Releasing a permit for the next operation to detect the error
                            concurrencyLimiter.release();

                            return;
                        }

                        inputSize.addAndGet(header.length);

                        setHeader(header);

                        // Allowing next IO operation
                        nextLatch.open();
                    }
                });
            });
        }

        private synchronized void readBlock() {
            checkException();

            LambdaLatch previousLatch = currentIOLatch;
            LambdaLatch nextLatch = currentIOLatch = new LambdaLatch();

            // Creating unpopulated block and adding it to the queue
            // Block is created before actual parsing to preserve the order
            Block<O> block = new Block<>();
            blocks.offer(block);

            previousLatch.setCallback(() -> // IO operation will be enqueued after the previous one finished
                    concurrencyLimiter.acquire( // and after there will be an execution slot available
                            () -> {
                                if (closed)
                                    return;

                                // Cancel all block deserialization requests if EOF was detected in the previous thread
                                if (eof) {
                                    block.eof = true;
                                    block.latch.countDown();
                                    // Do not release next latch
                                    return;
                                }

                                byte[] currentHeader = nextHeader;
                                byte[] blockAndNextHeader = new byte[getNextBlockLength() + BLOCK_HEADER_SIZE];
                                ByteBuffer buffer = ByteBuffer.wrap(blockAndNextHeader);
                                long ioStart = System.nanoTime();
                                channel.read(buffer, null, new CHAbstract() {
                                    @Override
                                    public void completed(Integer result, Object attachment) {
                                        // Recording time spent waiting for io operation completion
                                        ioDelayNanos.addAndGet(System.nanoTime() - ioStart);

                                        try {
                                            long start = System.nanoTime();

                                            if (closed)
                                                return;

                                            // Assert
                                            if (result != blockAndNextHeader.length) {
                                                exception = new RuntimeException("Premature EOF.");
                                                exception.printStackTrace();

                                                // TODO ?????????
                                                // Releasing a permit for the next operation to detect the error
                                                concurrencyLimiter.release();

                                                return;
                                            }

                                            // Extracting next header from the blob
                                            byte[] header = Arrays.copyOfRange(
                                                    blockAndNextHeader,
                                                    blockAndNextHeader.length - BLOCK_HEADER_SIZE,
                                                    blockAndNextHeader.length);
                                            setHeader(header);

                                            // Releasing next IO operation
                                            nextLatch.open();

                                            // CPU intensive task
                                            block.content = deserializeBlock(currentHeader, blockAndNextHeader);

                                            // Recording total deserialization time
                                            totalDeserializationNanos.addAndGet(System.nanoTime() - start);

                                            // Signaling that this block is populated
                                            block.latch.countDown();

                                            // Releasing acquired concurrency unit
                                            concurrencyLimiter.release();
                                        } catch (Exception e) {
                                            exception = e;
                                            // Releasing the latch for the block, to prevent deadlock in nextBlockOrClose
                                            block.latch.countDown();
                                            throw new RuntimeException(e);
                                        }
                                    }
                                });
                            })
            );
        }

        private synchronized void readBlocksIfNeeded() {
            while (blocks.size() < readAheadBlocks) {
                readBlock();
            }
        }

        private int getNextBlockLength() {
            return readIntBE(nextHeader, 9);
        }

        private void setHeader(byte[] header) {
            this.nextHeader = header;
            if (header[0] == 0)
                eof = true;
        }

        private void nextBlockOrClose() {
            checkException();

            try {
                Block<O> block = blocks.pop();
                readBlocksIfNeeded();
                block.latch.await();
                checkException();
                if (block.eof)
                    close();
                else
                    currentBlock = CUtils.asOutputPort(block.content);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public synchronized O take() {
            while (true) {
                if (closed)
                    return null;

                if (currentBlock == null) {
                    nextBlockOrClose();
                    if (closed)
                        return null;
                }

                O obj = currentBlock.take();
                if (obj == null) {
                    nextBlockOrClose();
                    continue;
                }
                return obj;
            }

        }

        @Override
        public synchronized void close() {
            closed = true;

            try {
                if (closeUnderlyingChannel)
                    channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            checkException();
        }
    }

    private static final class Block<O> {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile boolean eof;
        volatile List<O> content;
    }
}
