/*
 * Copyright 2020 MiLaboratory, LLC
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
package com.milaboratory.util.sorting;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import cc.redberry.pipe.util.Chunk;
import com.milaboratory.util.ObjectSerializer;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * Created by poslavsky on 28/02/2017.
 */
public final class Sorter<T> {
    private final ExecutorService executor;
    private final OutputPort<T> initialSource;
    private final Comparator<T> comparator;
    private final int chunkSize;
    private final ObjectSerializer<T> serializer;
    private final File tempFile;
    private final TLongArrayList chunkOffsets = new TLongArrayList();
    private boolean built = false;
    private int lastChunkSize = -1;
    /**
     * Amount of memory that can be used during read stage. Determined automatically as maximal block size during block
     * sort procedure.
     */
    private long memoryBudget = -1;

    public Sorter(OutputPort<T> initialSource, Comparator<T> comparator, int chunkSize,
                  ObjectSerializer<T> serializer, File tempFile, ExecutorService executor) {
        this.initialSource = initialSource;
        this.comparator = comparator;
        this.chunkSize = chunkSize;
        this.serializer = serializer;
        this.tempFile = tempFile;
        this.executor = executor;
    }

    /**
     * Sort objects supporting PrimitivIO serialization.
     */
    public static <T> OutputPortCloseable<T> sort(
            OutputPort<T> initialSource,
            Comparator<T> comparator,
            int chunkSize,
            Class<T> clazz,
            File tempFile) throws IOException {
        return sort(initialSource, comparator, chunkSize,
                new ObjectSerializer.PrimitivIOObjectSerializer<>(clazz), tempFile);
    }

    public static <T> OutputPortCloseable<T> sort(
            OutputPort<T> initialSource,
            Comparator<T> comparator,
            int chunkSize,
            ObjectSerializer<T> serializer,
            File tempFile) throws IOException {
        Sorter<T> sorter = new Sorter<>(initialSource, comparator, chunkSize, serializer, tempFile,
                ForkJoinPool.commonPool());
        sorter.build();
        return sorter.getSorted();
    }

    public void build() throws IOException {
        try (CountingOutputStream output = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile), 1024 * 1024))) {
            OutputPort<Chunk<T>> chunked = CUtils.buffered(CUtils.chunked(initialSource, chunkSize), 1);
            Chunk<T> chunk;
            // Maximal block size
            long maxBlockSize = 0;
            long previousPosition = 0;
            CountDownLatch currentBlockWriteLatch = new CountDownLatch(0);
            while ((chunk = chunked.take()) != null) {
                final Object[] data = chunk.toArray();

                if (data.length > 3000) // Empirical value learned from https://stackoverflow.com/a/17328147/769192
                    Arrays.parallelSort(data, (Comparator) comparator);
                else
                    Arrays.sort(data, (Comparator) comparator);

                maxBlockSize = Math.max(maxBlockSize, output.getByteCount() - previousPosition);
                previousPosition = output.getByteCount();

                // Waiting previous block to be fully flushed to the stream
                currentBlockWriteLatch.await();
                final CountDownLatch finalLatch = currentBlockWriteLatch = new CountDownLatch(1);

                // Initiating block serialization in a separate thread
                executor.submit(() -> {
                    chunkOffsets.add(output.getByteCount());
                    serializer.write((Collection) Arrays.asList(data), new CloseShieldOutputStream(output));
                    finalLatch.countDown();
                });

                // Tracking last chunk size
                lastChunkSize = data.length;
            }

            // Waiting last block to be written
            currentBlockWriteLatch.await();

            memoryBudget = maxBlockSize;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        built = true;
    }

    public OutputPortCloseable<T> getSorted() throws IOException {
        if (!built)
            throw new IllegalStateException("Invoke build before requesting results.");
        if (lastChunkSize == -1)
            // Empty output port removing temp file on close.
            return new OutputPortCloseable<T>() {
                @Override
                public void close() {
                    tempFile.delete();
                }

                @Override
                public T take() {
                    return null;
                }
            };
        else
            return new MergeSortingPort();
    }

    private final class MergeSortingPort implements OutputPortCloseable<T> {
        final PriorityQueue<SortedBlockReader> queue = new PriorityQueue<>();

        public MergeSortingPort() throws IOException {
            // There will be chunkOffsets.size() separate readers =>
            // chunkOffsets.size() separate buffered streams =>
            // consuming memoryBudget / chunkOffsets.size() bytes each, will give
            // ~ memoryBudget bytes consumed in total
            int bufferSize = (int) Math.min(
                    Math.max(1024,
                            memoryBudget / chunkOffsets.size()),
                    Integer.MAX_VALUE);
            for (int i = 0; i < chunkOffsets.size(); i++) {
                SortedBlockReader block = new SortedBlockReader(tempFile,
                        chunkOffsets.get(i),
                        i == chunkOffsets.size() - 1 ? lastChunkSize : chunkSize,
                        bufferSize);
                block.advance();
                queue.add(block);
            }
        }

        @Override
        public synchronized T take() {
            if (queue.isEmpty())
                return null;

            SortedBlockReader head = queue.poll();
            T current = head.current();

            try {
                // Advance the reader
                head.advance();
                if (head.current() != null)  // If reader has more records put it back to queue
                    queue.add(head);
                else  // If reader was completely drained close it and don't put it back to queue
                    head.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return current;
        }

        private boolean closed = false;

        @Override
        public synchronized void close() {
            if (closed)
                return;
            for (SortedBlockReader block : queue)
                try {
                    block.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            tempFile.delete();
            closed = true;
        }
    }

    private final class SortedBlockReader implements Comparable<SortedBlockReader>, AutoCloseable, Closeable {
        final DataInputStream input;
        final int chunkSize;
        private int position = 0;
        private final OutputPort<T> port;
        private T current = null;

        public SortedBlockReader(File file,
                                 long chunkOffset,
                                 int chunkSize,
                                 int bufferSize) throws IOException {
            this.chunkSize = chunkSize;

            final FileInputStream fo = new FileInputStream(file);
            // Setting file position to the beginning of the chunkId-th chunk
            fo.getChannel().position(chunkOffset);
            this.input = new DataInputStream(new BufferedInputStream(fo, bufferSize));
            this.port = serializer.read(this.input);
        }

        public void advance() throws IOException {
            if (position == chunkSize)
                current = null;
            else {
                ++position;
                current = port.take();
            }
        }

        public T current() {
            return current;
        }

        @Override
        public void close() throws IOException {
            this.input.close();
        }

        @Override
        public int compareTo(SortedBlockReader o) {
            return comparator.compare(current, o.current);
        }
    }

}
