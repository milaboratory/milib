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
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


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
 *
 * This object by itself does not hold any system resources, and there is no need to close it after use.
 * {@link Writer} instances produces by this class, in contrast, requires proper management
 * (e.g. has to be used inside try-with-resources).
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
     * Limits the concurrency for the operations produced by this object.
     *
     * Limits concurrent block operations, both CPU serialization and IO operations,
     * so no matter on what stage there is a bottle-neck, eventually back-pressure will be communicated to the
     * writing thread in a form of thread blocking.
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
     * ~ pure function
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
            activeRWs.incrementAndGet();
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

        public void run(Consumer<AsynchronousByteChannel> lambda) {
            run(lambda, false);
        }

        /**
         * Insert execution of a custom code into sequence of IO operations
         */
        public synchronized void run(Consumer<AsynchronousByteChannel> lambda, boolean executeInErrorState) {
            checkException();

            LambdaLatch previousLatch = currentWriteLatch;
            LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();
            previousLatch.setCallback(() -> {
                try {
                    if (executeInErrorState || stateOk())
                        lambda.accept(channel);
                } catch (Exception e) {
                    _ex(e);
                } finally {
                    nextLatch.open();
                }
            });
        }

        public synchronized void write(O obj) {
            buffer.add(obj);
            if (blockIsFull(buffer.size())) flush();
        }

        /**
         * Wait for all async operations to finish
         */
        public synchronized void sync() {
            try {
                checkException();
                final CountDownLatch latch = new CountDownLatch(1);
                run(__ -> latch.countDown(), true);
                latch.await();
                checkException();
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
                throw new IllegalStateException("Underlying channel does not support position information retrieval.");
        }

        private void acquireConcurrencyUnit() {
            // Checking for errors before and after throttling
            checkException();

            // Concurrency limits the whole block lifecycle (serialization + IO)
            // so operations will be throttled both on IO and CPU
            //
            // This operation will block caller thread if concurrency unit is not available at the moment
            // (this is the one out of the two blocking operations for the whole PrimitivIOBlocks suite)
            //
            // This blocking provides back-pressure for the writer thread in case of:
            //   - slow IO
            //   - serialization threads cant keep up with the demand for block serialization
            concurrencyLimiter.acquireUninterruptibly();

            // Checking for errors before and after throttling
            checkException();
        }

        private void scheduleIOOperation(LambdaLatch previousLatch, LambdaLatch nextLatch, ByteBuffer block) {

            /*
             * This method must release:
             *   - exactly one concurrencyLimiter unit
             *   - open nextLatch
             * across all it's sync/async execution branches
             */

            int blockBytes = block.limit();

            pendingOps.incrementAndGet();

            previousLatch.setCallback(() -> {
                pendingOps.decrementAndGet();

                // Cancelling current task if something is wrong
                if (!stateOk()) {
                    // Releasing a permit for the next operation
                    concurrencyLimiter.release();
                    // Opening latch for the next IO operation
                    nextLatch.open();
                    return;
                }

                ongoingIOOps.incrementAndGet();
                long ioBegin = System.nanoTime();
                channel.write(block, null,
                        new CHAbstract(nextLatch) {
                            @Override
                            public void completed(Integer result, Object attachment) {
                                ioDelayNanos.addAndGet(System.nanoTime() - ioBegin);
                                ongoingIOOps.decrementAndGet();

                                // Tracking output size
                                outputSize.addAndGet(blockBytes);

                                // Assert
                                if (result != blockBytes)
                                    _ex(new RuntimeException(
                                            "Wrong block size. (result = " + result +
                                                    "; blockSize = " + blockBytes + ")"));

                                // Releasing a permit for the next operation
                                concurrencyLimiter.release();

                                // Opening latch for the next IO operation
                                nextLatch.open();
                            }
                        });
            });
        }

        private boolean stateOk() {
            return !(closed || exception != null);
        }

        public synchronized void writeHeader(PrimitivIOBlockHeader header) {
            if (!buffer.isEmpty())
                throw new IllegalStateException("Buffer is not empty. Invoke flush() before writeHeader(...).");

            acquireConcurrencyUnit();

            // Creating latches for IO operations ordering
            LambdaLatch previousLatch = currentWriteLatch;
            LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();

            // Header bytes
            ByteBuffer block = header.asByteBuffer();

            scheduleIOOperation(previousLatch, nextLatch, block);
        }

        public synchronized void writeBlock(final List<O> content) {
            acquireConcurrencyUnit();

            // Creating latches for IO operations ordering
            LambdaLatch previousLatch = currentWriteLatch;
            LambdaLatch nextLatch = currentWriteLatch = new LambdaLatch();

            long submittedTimestamp = System.nanoTime();
            executor.submit(() -> {
                concurrencyOverhead.addAndGet(System.nanoTime() - submittedTimestamp);
                try {

                    // CPU intensive operation, no IO
                    long start = System.nanoTime();
                    ByteBuffer block = serializeBlock(content);
                    totalSerializationNanos.addAndGet(System.nanoTime() - start);

                    scheduleIOOperation(previousLatch, nextLatch, block);

                } catch (RuntimeException e) { // From serializeBlock

                    _ex(e);

                    // Releasing a permit for the next operation to detect the error
                    concurrencyLimiter.release();
                    // Opening latch for the next IO operation
                    nextLatch.open();

                }
            });
        }

        @Override
        public synchronized void close() {
            try {
                if (closed)
                    return;

                if (!buffer.isEmpty())
                    // Writing leftovers
                    flush();

                // Writing final header
                writeHeader(PrimitivIOBlockHeader.lastHeader());

                // Waiting EOF header to be flushed to the stream
                sync();

                closed = true;
                activeRWs.decrementAndGet();

                if (closeUnderlyingChannel)
                    channel.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public AsynchronousByteChannel getChannel() {
            return channel;
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

    protected abstract class CHAbstract implements CompletionHandler<Integer, Object> {
        final LambdaLatch nextLambdaLatch;

        /**
         * @param nextLambdaLatch nonnull required, will be automatically released on error
         */
        public CHAbstract(LambdaLatch nextLambdaLatch) {
            Objects.requireNonNull(nextLambdaLatch);
            this.nextLambdaLatch = nextLambdaLatch;
        }

        @Override
        public void failed(Throwable exc, Object attachment) {
            _ex(exc);

            // For correct statistics calculation
            ongoingIOOps.decrementAndGet();

            // Releasing a permit for the next operation
            concurrencyLimiter.release();

            // Releasing all the locks associated with this IO operation
            nextLambdaLatch.open();
        }
    }
}
