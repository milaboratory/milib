package com.milaboratory.util;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import gnu.trove.list.array.TLongArrayList;

import java.io.*;
import java.nio.file.Path;
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

    private final Path tempFilePrefix;

    public Sorter2(OutputPort<O> initialSource, Function<O, K> keyExtractor,
                   Comparator<K> comparator, Supplier<Serializer<O>> oSerializerSupplier,
                   Supplier<Serializer<K>> kSerializerSupplier, ExecutorService executor,
                   int nThreads, int nOpenFileStreams, long chunkSize,
                   Path tempFilePrefix) {
        this.initialSource = initialSource;
        this.keyExtractor = keyExtractor;
        this.comparator = comparator;
        this.oSerializerSupplier = oSerializerSupplier;
        this.kSerializerSupplier = kSerializerSupplier;
        this.executor = executor;
        this.nThreads = nThreads;
        this.nOpenFileStreams = nOpenFileStreams;
        this.chunkSize = chunkSize;
        this.tempFilePrefix = tempFilePrefix;
    }

    @SuppressWarnings("unchecked")
    public OutputPortCloseable<O> run() throws IOException {

        ArrayList<KO> koChunk = new ArrayList<>();
        List<KO> koChunkSync = Collections.synchronizedList(koChunk);

        Sorter2<K, O>.SerWorker[] workers = new Sorter2.SerWorker[nThreads];
        for (int i = 0; i < nThreads; ++i)
            workers[i] = new SerWorker();

        TLongArrayList chunkOffsets = new TLongArrayList();
        chunkOffsets.add(0);

        int fileIndex = 0;

        File initialFile = tempFilePrefix.resolveSibling(tempFilePrefix.getFileName().toString() + (fileIndex++)).toFile();

        try (OutputStream tempFileStream = new BufferedOutputStream(new FileOutputStream(initialFile))) {
            AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
            long offset = 0;

            do {
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

                if (koChunk.isEmpty())
                    break;

                // sorting
                KO[] kos = koChunk.toArray(new Sorter2.KO[0]);
                Arrays.parallelSort(kos, (a, b) -> comparator.compare(a.key, b.key));

                for (KO ko : kos) {
                    offset += ko.data.length;
                    tempFileStream.write(ko.data);
                }

                tempFileStream.write(0);
                tempFileStream.write(0);
                tempFileStream.write(0);
                tempFileStream.write(0);
                offset += 4;

                chunkOffsets.add(offset);
            } while (!sourceIsEmpty.get());

        }

        chunkOffsets.removeAt(chunkOffsets.size() - 1);

        File sourceFile = initialFile;

        Serializer<K> kSerializer = kSerializerSupplier.get();
        Serializer<O> oSerializer = oSerializerSupplier.get();

        while (nOpenFileStreams < chunkOffsets.size()) {
            TLongArrayList newChunkOffsets = new TLongArrayList();
            File destFile = tempFilePrefix.resolveSibling(tempFilePrefix.getFileName().toString() + (fileIndex++)).toFile();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(destFile))) {
                int lastChunk = 0;
                long offset = 0;
                newChunkOffsets.add(offset);
                while (lastChunk < chunkOffsets.size()) {
                    int nChunks = Math.min(nOpenFileStreams, chunkOffsets.size() - lastChunk);
                    MergeSortingPort r = new MergeSortingPort(sourceFile, chunkOffsets, lastChunk, nChunks, kSerializer, null);
                    for (KO ko : CUtils.it(r)) {
                        os.write(ko.data);
                        offset += ko.data.length;
                    }
                    os.write(0);
                    os.write(0);
                    os.write(0);
                    os.write(0);
                    offset += 4;
                    newChunkOffsets.add(offset);
                    lastChunk += nChunks;
                }
            }
            sourceFile.delete();
            sourceFile = destFile;
            chunkOffsets = newChunkOffsets;
            chunkOffsets.removeAt(chunkOffsets.size() - 1);
        }

        File finalSourceFiles = sourceFile;
        MergeSortingPort r = new MergeSortingPort(finalSourceFiles, chunkOffsets, 0, chunkOffsets.size(), kSerializer, oSerializer);
        return new OutputPortCloseable<O>() {
            @Override
            public void close() {
                r.close();
                finalSourceFiles.delete();
            }

            @Override
            public O take() {
                KO ko = r.take();
                if (ko == null)
                    return null;
                return ko.obj;
            }
        };
    }

    private final class MergeSortingPort implements OutputPortCloseable<KO> {
        final PriorityQueue<Sorter2.SortedBlockReader> queue = new PriorityQueue<>();

        public MergeSortingPort(File tempFile, TLongArrayList chunkOffsets, int firstChunk, int nChunks,
                                Serializer<K> kSerializer,
                                Serializer<O> oSerializer) throws IOException {
            int bufferSize = (int) Math.min(
                    Math.max(1024, chunkSize / nChunks),
                    Integer.MAX_VALUE);

            for (int i = 0; i < nChunks; i++) {
                SortedBlockReader block = new SortedBlockReader(tempFile,
                        chunkOffsets.get(firstChunk + i),
                        bufferSize, kSerializer, oSerializer);
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

            closed = true;
        }
    }

    private final class SortedBlockReader implements Comparable<SortedBlockReader>, AutoCloseable, Closeable {
        final BufferedInputStream in;
        final DataInputStream input;
        final int bufferSize;
        final Serializer<K> kSerializer;
        final Serializer<O> oSerializer;
        private KO current = null;

        public SortedBlockReader(File file,
                                 long chunkOffset,
                                 int bufferSize,
                                 Serializer<K> kSerializer,
                                 Serializer<O> oSerializer) throws IOException {

            final FileInputStream fo = new FileInputStream(file);
            // Setting file position to the beginning of the chunkId-th chunk
            fo.getChannel().position(chunkOffset);
            this.kSerializer = kSerializer;
            this.oSerializer = oSerializer;
            this.bufferSize = bufferSize;
            this.in = new BufferedInputStream(fo, bufferSize);
            this.input = new DataInputStream(in);
        }

        public void advance() {
            try {
                if (oSerializer != null) {
                    int len = input.readInt();
                    if (len == 0) {
                        current = null;
                        return;
                    }
                    K k = kSerializer.deserialize(this.input);
                    O o = oSerializer.deserialize(this.input);
                    current = new KO(k, o);
                } else {
                    in.mark(bufferSize);
                    int len = input.readInt();
                    if (len == 0) {
                        current = null;
                        return;
                    }
                    K k = kSerializer.deserialize(this.input);
                    in.reset();
                    byte[] data = new byte[len];
                    input.readFully(data);
                    current = new KO(k, data);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            return comparator.compare(current.key, o.current.key);
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

                    try {
                        kSerializer.serialize(key, dos);
                        oSerializer.serialize(obj, dos);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    byte[] data = bb.toByteArray();
                    writeInt(data, 0, data.length);

                    dest.add(new KO(key, data));
                    if (bytesWritten.addAndGet(data.length) > chunkSize)
                        return;
                }
                sourceEmpty.set(true);
            };
        }
    }

    private static void writeInt(byte[] data, int index, int value) {
        data[index + 0] = (byte) ((value >>> 24) & 0xFF);
        data[index + 1] = (byte) ((value >>> 16) & 0xFF);
        data[index + 2] = (byte) ((value >>> 8) & 0xFF);
        data[index + 3] = (byte) ((value >>> 0) & 0xFF);
    }

    private final class KO {
        final K key;
        final O obj;
        final byte[] data;

        KO(K key, byte[] data) {
            this.key = key;
            this.obj = null;
            this.data = data;
        }

        KO(K key, O o) {
            this.key = key;
            this.obj = o;
            this.data = null;
        }
    }

    public interface Serializer<T> {
        void serialize(T t, DataOutput dest) throws IOException;

        T deserialize(DataInput source) throws IOException;
    }
}
