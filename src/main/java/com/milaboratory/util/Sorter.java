package com.milaboratory.util;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import cc.redberry.pipe.util.Chunk;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by poslavsky on 28/02/2017.
 */
public final class Sorter<T> {
    private final OutputPort<T> initialSource;
    private final Comparator<T> comparator;
    private final int chunkSize;
    private final ObjectSerializer<T> serializer;
    private final File tempFile;

    private final TLongArrayList chunkOffsets = new TLongArrayList();
    private int lastChunkSize = -1;


    Sorter(OutputPort<T> initialSource, Comparator<T> comparator, int chunkSize, ObjectSerializer<T> serializer, File tempFile) {
        this.initialSource = initialSource;
        this.comparator = comparator;
        this.chunkSize = chunkSize;
        this.serializer = serializer;
        this.tempFile = tempFile;
    }

    public static <T> OutputPortCloseable<T> sort(
            OutputPort<T> initialSource,
            Comparator<T> comparator,
            int chunkSize,
            ObjectSerializer<T> serializer,
            File tempFile) throws IOException {
        Sorter<T> sorter = new Sorter<>(initialSource, comparator, chunkSize, serializer, tempFile);
        sorter.build();
        return sorter.getSorted();
    }

    void build() throws IOException {
        try(CountingOutputStream output = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile), 1024 * 1024))) {
            OutputPort<Chunk<T>> chunked = CUtils.buffered(CUtils.chunked(initialSource, chunkSize), 1);
            Chunk<T> chunk;
            while ((chunk = chunked.take()) != null) {
                Object[] data = chunk.toArray();
                Arrays.sort(data, (Comparator) comparator);
                chunkOffsets.add(output.getByteCount());
                serializer.write((Collection) Arrays.asList(data), new CloseShieldOutputStream(output));
                lastChunkSize = data.length;
            }
        }
    }

    OutputPortCloseable<T> getSorted() throws IOException {
        return new MergeSortingPort();
    }

    private final class MergeSortingPort implements OutputPortCloseable<T> {
        final PriorityQueue<SortedBlockReader> queue = new PriorityQueue<>();

        public MergeSortingPort() throws IOException {
            for (int i = 0; i < chunkOffsets.size(); i++) {
                SortedBlockReader block = new SortedBlockReader(tempFile, chunkOffsets.get(i), i == chunkOffsets.size() - 1 ? lastChunkSize : chunkSize);
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
                                 int chunkSize) throws IOException {
            this.chunkSize = chunkSize;

            final FileInputStream fo = new FileInputStream(file);
            // Setting file position to the beginning of the chunkId-th chunk
            fo.getChannel().position(chunkOffset);
            this.input = new DataInputStream(new BufferedInputStream(fo, 1024));
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
