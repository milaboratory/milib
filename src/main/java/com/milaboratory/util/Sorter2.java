package com.milaboratory.util;

import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import gnu.trove.list.array.TLongArrayList;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 */
public class Sorter2<K, O> {
    private final OutputPort<O> initialSource;
    private final Function<O, K> keyExtractor;
    private final Comparator<K> comparator;
    private final Supplier<Serializer<O>> oSerializerSupplier;
    private final Supplier<Serializer<K>> kSerializerSupplier;

    private final ExecutorService executor;

    private final int nThreads;
    private final int nOpenFileStreams; // number of open reads from temp file
    private final long chunkSize; // in bytes

    private final File tempFile;

    public Sorter2(OutputPort<O> initialSource, Function<O, K> keyExtractor, Comparator<K> comparator, Supplier<Serializer<O>> oSerializerSupplier, Supplier<Serializer<K>> kSerializerSupplier, ExecutorService executor, int nThreads, int nOpenFileStreams, long chunkSize, File tempFile) {
        this.initialSource = initialSource;
        this.keyExtractor = keyExtractor;
        this.comparator = comparator;
        this.oSerializerSupplier = oSerializerSupplier;
        this.kSerializerSupplier = kSerializerSupplier;
        this.executor = executor;
        this.nThreads = nThreads;
        this.nOpenFileStreams = nOpenFileStreams;
        this.chunkSize = chunkSize;
        this.tempFile = tempFile;
    }

    @SuppressWarnings("unchecked")
    public void run() throws IOException {

        ArrayList<KO> koChunk = new ArrayList<>();
        List<KO> koChunkSync = Collections.synchronizedList(koChunk);

        Sorter2<K, O>.SerWorker[] workers = new Sorter2.SerWorker[nThreads];
        for (int i = 0; i < nThreads; ++i)
            workers[i] = new SerWorker();

        TLongArrayList chunkOffsets = new TLongArrayList();
        chunkOffsets.add(0);

        AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
        do {
            OutputStream tempFileStream = new BufferedOutputStream(new FileOutputStream(tempFile));
            koChunk.clear();

            AtomicLong bytesWritten = new AtomicLong(0);
            Arrays.stream(workers)
                    .map(w -> executor.submit(w.serialize(koChunkSync, bytesWritten, sourceIsEmpty)))
                    .forEach(f -> {
                        try {
                            f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });

            // sorting
            KO[] kos = koChunk.toArray(new Sorter2.KO[0]);
            Arrays.parallelSort(kos, (a, b) -> comparator.compare(a.k, b.k));

            long offset = 0;
            for (KO ko : kos) {
                offset += ko.data.length;
                tempFileStream.write(ko.data);
            }

            tempFileStream.write(0);
            tempFileStream.write(0);
            tempFileStream.write(0);
            tempFileStream.write(0);

            chunkOffsets.add(offset);
        } while (!sourceIsEmpty.get());


        // io-io-io
    }

    public OutputPortCloseable<KO> getSorted(File tempFile) {
        // Empty output port removing temp file on close.
        return new OutputPortCloseable<KO>() {
            @Override
            public void close() {
                tempFile.delete();
            }

            @Override
            public KO take() {
                return null;
            }
        };
    }

    private final class MergeSortingPort implements OutputPortCloseable<KO> {
        final PriorityQueue<Sorter2.SortedBlockReader> queue = new PriorityQueue<>();

        public MergeSortingPort(File tempFile, TLongArrayList chunkOffsets, int firstChunk, int nChunks, Serializer<K> kSerializer) throws IOException {
            // There will be chunkOffsets.size() separate readers =>
            // chunkOffsets.size() separate buffered streams =>
            // consuming memoryBudget / chunkOffsets.size() bytes each, will give
            // ~ memoryBudget bytes consumed in total
            int bufferSize = (int) Math.min(
                    Math.max(1024, chunkSize / nChunks),
                    Integer.MAX_VALUE);

            for (int i = 0; i < nChunks; i++) {
                SortedBlockReader block = new SortedBlockReader(tempFile,
                        chunkOffsets.get(firstChunk + i),
                        bufferSize, kSerializer);
                block.advance();
                queue.add(block);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public synchronized KO take() {
            if (queue.isEmpty())
                return null;

            SortedBlockReader head = queue.poll();
            KO current = head.current();

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
        @SuppressWarnings("unchecked")
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
        final BufferedInputStream in;
        final DataInputStream input;
        final int bufferSize;
        final Serializer<K> kSerializer;
        private KO current = null;

        public SortedBlockReader(File file,
                                 long chunkOffset,
                                 int bufferSize,
                                 Serializer<K> kSerializer) throws IOException {

            final FileInputStream fo = new FileInputStream(file);
            // Setting file position to the beginning of the chunkId-th chunk
            fo.getChannel().position(chunkOffset);
            this.kSerializer = kSerializer;
            this.bufferSize = bufferSize;
            this.in = new BufferedInputStream(fo, bufferSize);
            this.input = new DataInputStream(in);
        }

        public void advance() {
            try {
                in.mark(bufferSize);
                int len = input.readInt();
                if (len == 0) {
                    current = null;
                    return;
                }
                K k = kSerializer.deserialize(SortedBlockReader.this.input);
                in.reset();
                byte[] data = new byte[len];
                input.readFully(data);
                current = new KO(k, data);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }

        public KO current() {
            return current;
        }

        @Override
        public void close() throws IOException {
            this.input.close();
        }

        @Override
        public int compareTo(SortedBlockReader o) {
            return comparator.compare(current.k, o.current.k);
        }
    }

    private final class SerWorker {
        final Serializer<O> oSerializer = oSerializerSupplier.get();
        final Serializer<K> kSerializer = kSerializerSupplier.get();
        final ByteArrayOutputStream bb = new ByteArrayOutputStream();


        Runnable serialize(List<KO> dest, AtomicLong bytesWritten, AtomicBoolean sourceEmpty) {
            return () -> {
                DataOutputStream dos = new DataOutputStream(bb);
                O obj;
                while ((obj = initialSource.take()) != null) {
                    K key = keyExtractor.apply(obj);
                    bb.reset();

                    bb.write(0);
                    bb.write(0);
                    bb.write(0);
                    bb.write(0);

                    kSerializer.serialize(key, dos);

                    oSerializer.serialize(obj, dos);
                    byte[] data = bb.toByteArray();
                    int len = data.length;

                    data[0] = (byte) ((len >>> 24) & 0xFF);
                    data[1] = (byte) ((len >>> 16) & 0xFF);
                    data[2] = (byte) ((len >>> 8) & 0xFF);
                    data[3] = (byte) ((len >>> 0) & 0xFF);

                    dest.add(new KO(key, data));
                    if (bytesWritten.addAndGet(data.length) > chunkSize)
                        return;
                }
                sourceEmpty.set(true);
            };
        }
    }


    private final class KO {
        final K k;
        final byte[] data;

        KO(K k, byte[] data) {
            this.k = k;
            this.data = data;
        }
    }

    public interface Serializer<T> {
        void serialize(T t, DataOutput dest);

        T deserialize(DataInput source);
    }
}
