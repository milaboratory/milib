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
import java.nio.channels.CompletionHandler;
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
import java.util.function.Function;

import static com.milaboratory.primitivio.blocks.PrimitivIHeaderActions.*;

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
     * @param clazz        class to deserialize
     * @param executor     executor to execute serialization process in
     *                     (the same executor service as used in target AsynchronousByteChannels is recommended)
     * @param concurrency  maximal number of concurrent deserializations, actual concurrency level is also limited by
     *                     readAheadBlocks parameter (effective concurrency will be ~min(readAheadBlocks, concurrency))
     *                     and IO speed
     * @param inputState   stream state
     * @param decompressor block decompressor
     */
    public PrimitivIBlocks(Class<O> clazz, ExecutorService executor, int concurrency,
                           PrimitivIState inputState, LZ4FastDecompressor decompressor) {
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
    private List<O> deserializeBlock(PrimitivIOBlockHeader header, byte[] blockAndNextHeader) {
        // Reading header
        int numberOfObjects = header.getNumberOfObjects();
        int blockLength = blockAndNextHeader.length - BLOCK_HEADER_SIZE;
        assert blockLength == header.getDataSize();

        inputSize.addAndGet(blockAndNextHeader.length);

        // Stats {
        long start = System.nanoTime();
        // }

        byte[] data;
        int dataLen;
        if (header.isCompressed()) { // Compressed block
            int decompressedLength = header.getUncompressedDataSize();
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

        if (actualChecksum != header.getChecksum())
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

    public Reader newReader(Path path, int readAheadBlocks) throws IOException {
        return newReader(path, readAheadBlocks, skipAll());
    }

    public Reader newReader(Path path, int readAheadBlocks,
                            Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) throws IOException {
        return newReader(createAsyncChannel(path), readAheadBlocks, 0,
                specialHeaderAction, true);
    }

    public Reader newReader(AsynchronousFileChannel channel, int readAheadBlocks, long position) {
        return newReader(channel, readAheadBlocks, position, skipAll(), false);
    }

    public Reader newReader(AsynchronousFileChannel channel, int readAheadBlocks, long position,
                            Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction) {
        return newReader(channel, readAheadBlocks, position, specialHeaderAction, false);
    }

    public Reader newReader(AsynchronousFileChannel channel, int readAheadBlocks, long position,
                            Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction,
                            boolean closeUnderlyingChannel) {
        return newReader(new AsynchronousFileChannelAdapter(channel, position),
                readAheadBlocks, specialHeaderAction, closeUnderlyingChannel);
    }

    public Reader newReader(AsynchronousByteChannel channel, int readAheadBlocks,
                            Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction,
                            boolean closeUnderlyingChannel) {
        return new Reader(channel, readAheadBlocks, specialHeaderAction, closeUnderlyingChannel);
    }

    public final class Reader implements OutputPortCloseable<O> {
        // Parameters
        final AsynchronousByteChannel channel;
        final int readAheadBlocks;
        /**
         * Function that decides which action should be taken if special header is encountered
         */
        final Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction;
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
        volatile PrimitivIOBlockHeader nextHeader;

        /**
         * Blocks being red ahead
         */
        final ArrayDeque<Block<O>> blocks = new ArrayDeque<>();
        Block<O> currentBlock = null;
        volatile boolean closed = false;

        public Reader(AsynchronousByteChannel channel, int readAheadBlocks,
                      Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> specialHeaderAction,
                      boolean closeUnderlyingChannel) {
            this.channel = channel;
            this.readAheadBlocks = readAheadBlocks;
            this.specialHeaderAction = specialHeaderAction;
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

                _readHeader(nextLatch);
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

                                // Cancel all block deserialization requests if EOF was detected in the previous block
                                if (eof) {
                                    block.eof = true;
                                    block.latch.countDown();
                                    // Do not release next latch
                                    return;
                                }

                                block.header = nextHeader; // read as: current header
                                if (nextHeader.isSpecial()) {
                                    // Releasing special block
                                    block.latch.countDown();
                                    _readHeader(nextLatch);
                                } else
                                    _readBlock(nextHeader, block, nextLatch);
                            })
            );
        }

        /**
         * Low level read next header, to be executed from concurrencyLimiter.acquire
         *
         * @param nextLatch latch to open after IO is complete
         */
        private void _readHeader(LambdaLatch nextLatch) {
            byte[] headerBytes = new byte[BLOCK_HEADER_SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
            long ioStart = System.nanoTime();

            channel.read(buffer, null, new CHAbstractCL() {
                @Override
                public void completed(Integer result, Object attachment) {
                    try {
                        // Recording time spent waiting for io operation completion
                        ioDelayNanos.addAndGet(System.nanoTime() - ioStart);

                        if (closed)
                            return;

                        // Assert
                        if (result != BLOCK_HEADER_SIZE) {
                            _ex(new RuntimeException("Premature EOF."));
                            return;
                        }

                        inputSize.addAndGet(headerBytes.length);

                        setHeader(headerBytes);

                        // Allowing next IO operation
                        nextLatch.open();

                        // Releasing acquired concurrency unit
                        concurrencyLimiter.release();
                    } catch (Exception e) {
                        _ex(e);

                        throw new RuntimeException(e);
                    }
                }
            });
        }

        /**
         * Low level read block, to be executed from concurrencyLimiter.acquire
         *
         * @param currentHeader header of current block
         * @param block         block to populate
         * @param nextLatch     latch to open after IO is complete
         */
        private void _readBlock(PrimitivIOBlockHeader currentHeader,
                                Block<O> block, LambdaLatch nextLatch) {
            byte[] blockAndNextHeader = new byte[currentHeader.getDataSize() + BLOCK_HEADER_SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(blockAndNextHeader);
            long ioStart = System.nanoTime();

            channel.read(buffer, null, new CHAbstractCL() {
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
                            _ex(new RuntimeException("Premature EOF."));
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

                        // Signaling that the block is populated
                        block.latch.countDown();

                        // Releasing acquired concurrency unit
                        concurrencyLimiter.release();
                    } catch (Exception e) {
                        _ex(e);

                        // Releasing the latch for the block, to prevent deadlock in nextBlockOrClose
                        block.latch.countDown();

                        throw new RuntimeException(e);
                    }
                }
            });
        }

        private synchronized void readBlocksIfNeeded() {
            while (blocks.size() < readAheadBlocks) {
                readBlock();
            }
        }

        private void setHeader(byte[] headerBytes) {
            try {
                nextHeader = PrimitivIOBlockHeader.readHeaderNoCopy(headerBytes);
                if (nextHeader.isLastBlock())
                    eof = true;
            } catch (Exception ex) {
                _ex(ex);
            }
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
                else {
                    currentBlock = block;

                    // Block processing / initialization
                    if (currentBlock.header.isSpecial()) {
                        PrimitivIHeaderAction<O> action = specialHeaderAction.apply(currentBlock.header);
                        O obj;
                        if (isSkip(action))
                            currentBlock.port = CUtils.EMPTY_OUTPUT_PORT;
                        else if (isStopReading(action))
                            close();
                        else if ((obj = tryExtractOutputObject(action)) != null)
                            currentBlock.port = CUtils.asOutputPort(obj);
                        else
                            throw new RuntimeException("Unknown action type: " + action);
                    } else
                        currentBlock.port = CUtils.asOutputPort(block.content);
                }
            } catch (Exception e) {
                _ex(e);
            }
        }

        @Override
        public synchronized O take() {
            while (true) {
                if (closed)
                    return null;

                if (currentBlock == null) { // initial state
                    nextBlockOrClose();
                    continue;
                }

                O obj = currentBlock.port.take();
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

    private void _ex(Throwable ex) {
        exception = ex;

        // TODO excessive ???
        ex.printStackTrace();

        // Releasing a permit for the next operation to detect the error
        concurrencyLimiter.release();
    }

    protected abstract class CHAbstractCL implements CompletionHandler<Integer, Object> {
        @Override
        public void failed(Throwable exc, Object attachment) {
            _ex(exc);
        }
    }

    private static final class Block<O> {
        final CountDownLatch latch = new CountDownLatch(1);

        volatile boolean eof;
        volatile List<O> content;
        volatile PrimitivIOBlockHeader header;

        volatile OutputPort<O> port;
    }
}
