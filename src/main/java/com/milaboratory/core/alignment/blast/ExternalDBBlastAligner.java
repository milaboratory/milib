package com.milaboratory.core.alignment.blast;

import cc.redberry.pipe.InputPort;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import cc.redberry.pipe.blocks.Buffer;
import com.milaboratory.core.Range;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.alignment.batch.*;
import com.milaboratory.core.io.sequence.fasta.FastaWriter;
import com.milaboratory.core.mutations.Mutations;
import com.milaboratory.core.mutations.MutationsUtil;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;

/**
 * Blast aligner of query sequences with external (non-milib-managed) database of sequences.
 */
public class ExternalDBBlastAligner<S extends Sequence<S>> implements PipedBatchAligner<S, ExternalDBBlastHit<S>> {
    private static final String OUTFMT = "7 btop sstart send qstart qend score bitscore evalue stitle sseqid sseq";
    private static final String QUERY_ID_PREFIX = "Q";
    final BlastDB database;
    final Alphabet<S> alphabet;
    final BlastAlignerParameters parameters;

    public ExternalDBBlastAligner(BlastDB database) {
        this(database, null);
    }

    public ExternalDBBlastAligner(BlastDB database, BlastAlignerParameters parameters) {
        this.database = database;
        this.alphabet = (Alphabet<S>) database.getAlphabet();
        this.parameters = parameters == null ? new BlastAlignerParameters() : parameters;
        this.parameters.chechAlphabet(alphabet);
    }

    @Override
    public <Q> OutputPort<PipedAlignmentResult<ExternalDBBlastHit<S>, Q>> align(OutputPort<Q> input, SequenceExtractor<Q, S> extractor) {
        return new BlastWorker<>(input, extractor);
    }

    @Override
    public <Q extends HasSequence<S>> OutputPort<PipedAlignmentResult<ExternalDBBlastHit<S>, Q>> align(OutputPort<Q> input) {
        return new BlastWorker<>(input, BatchAlignmentUtil.DUMMY_EXTRACTOR);
    }

    /**
     * Supervisor of a single blast process. Spins up two separate threads for pushing input sequences to blast and
     * fetching alignment results and blast process by itself.
     */
    private class BlastWorker<Q> implements
            OutputPortCloseable<PipedAlignmentResult<ExternalDBBlastHit<S>, Q>> {
        final ConcurrentMap<String, Q> queryMapping = new ConcurrentHashMap<>();
        final Buffer<PipedAlignmentResult<ExternalDBBlastHit<S>, Q>> resultsBuffer;
        final Process process;
        final BlastSequencePusher<Q> pusher;
        final BlastResultsFetcher<Q> fetcher;

        public BlastWorker(OutputPort<Q> source, SequenceExtractor<Q, S> sequenceExtractor) {
            this.resultsBuffer = new Buffer<>(32);
            try {
                List<String> cmd = new ArrayList<>();

                cmd.addAll(asList(Blast.toBlastCommand(database.getAlphabet()),
                        "-db", database.getName(),
                        "-outfmt", OUTFMT));
                parameters.addArgumentsTo(cmd);

                ProcessBuilder processBuilder = Blast.getProcessBuilder(cmd);

                processBuilder.redirectErrorStream(false);
                parameters.addEnvVariablesTo(processBuilder);

                this.process = processBuilder.start();
                this.pusher = new BlastSequencePusher<>(source, sequenceExtractor,
                        queryMapping, this.process.getOutputStream());
                this.fetcher = new BlastResultsFetcher<>(this.resultsBuffer.createInputPort(),
                        queryMapping, this.process.getInputStream());

                this.pusher.start();
                this.fetcher.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public PipedAlignmentResult<ExternalDBBlastHit<S>, Q> take() {
            return resultsBuffer.take();
        }

        @Override
        public void close() {
            if (pusher.source instanceof OutputPortCloseable)
                ((OutputPortCloseable) pusher.source).close();
        }
    }

    /**
     * Fetches blast alignment results
     */
    private class BlastResultsFetcher<Q> extends Thread {
        final InputPort<PipedAlignmentResult<ExternalDBBlastHit<S>, Q>> resultsInputPort;
        final BufferedReader reader;
        final ConcurrentMap<String, Q> queryMapping;

        public BlastResultsFetcher(InputPort<PipedAlignmentResult<ExternalDBBlastHit<S>, Q>> resultsInputPort,
                                   ConcurrentMap<String, Q> queryMapping, InputStream stream) {
            this.resultsInputPort = resultsInputPort;
            this.reader = new BufferedReader(new InputStreamReader(stream));
            this.queryMapping = queryMapping;
        }

        @Override
        public void run() {
            try {
                String line;
                int num = -1;

                Q query = null;
                ArrayList<ExternalDBBlastHit<S>> hits = null;

                while ((line = reader.readLine()) != null) {
                    if (line.contains("hits found")) {
                        num = parseInt(line.replace("#", "").replace("hits found", "").trim());
                        hits = new ArrayList<>(num);
                    } else if (line.contains("Query")) {
                        String qid = line.replace("# Query: ", "").trim();
                        query = queryMapping.remove(qid);
                        if (query == null)
                            throw new RuntimeException();
                    } else if (!line.startsWith("#")) {
                        if (hits == null)
                            throw new RuntimeException();

                        hits.add(parseLine(line));
                    }

                    if (hits != null && hits.size() == num) {
                        if (query == null)
                            throw new RuntimeException();

                        resultsInputPort.put(new PipedAlignmentResultImpl<>(hits, query));

                        query = null;
                        hits = null;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                // Closing port
                resultsInputPort.put(null);
            }
        }

        private ExternalDBBlastHit<S> parseLine(String line) {
            // Parsing individual fields
            String[] fields = line.split("\t");
            int i = 0;
            String btop = fields[i++],
                    sstart = fields[i++],
                    send = fields[i++],
                    qstart = fields[i++],
                    qend = fields[i++],
                    score = fields[i++],
                    bitscore = fields[i++],
                    evalue = fields[i++],
                    stitle = fields[i++],
                    sseqid = fields[i++],
                    sseq = fields[i++].replace("-", "");

            // Converting mutations to MILib representation
            Mutations<S> mutations = new Mutations<>(alphabet, MutationsUtil.btopDecode(btop, alphabet));

            // Creating alignment
            Alignment<S> alignment = new Alignment<>(alphabet.parse(sseq), mutations,
                    new Range(0, sseq.length()), new Range(parseInt(qstart) - 1, parseInt(qend)),
                    Float.parseFloat(bitscore));

            // Parsing subject range
            Range sRange = new Range(parseInt(sstart) - 1, parseInt(send));

            // Return parsed hit
            return new ExternalDBBlastHit<>(alignment, Double.parseDouble(score), Double.parseDouble(bitscore),
                    Double.parseDouble(evalue), sRange, sseqid, stitle);
        }
    }

    /**
     * Pushes fasta formatted sequences to blast
     */
    private class BlastSequencePusher<Q> extends Thread {
        final AtomicLong counter = new AtomicLong();
        final OutputPort<Q> source;
        final SequenceExtractor<Q, S> sequenceExtractor;
        final ConcurrentMap<String, Q> queryMapping;
        final FastaWriter<S> writer;

        public BlastSequencePusher(OutputPort<Q> source, SequenceExtractor<Q, S> sequenceExtractor,
                                   ConcurrentMap<String, Q> queryMapping,
                                   OutputStream stream) {
            this.source = source;
            this.sequenceExtractor = sequenceExtractor;
            this.queryMapping = queryMapping;
            this.writer = new FastaWriter<S>(stream, FastaWriter.DEFAULT_MAX_LENGTH);
        }

        @Override
        public void run() {
            Q query;

            while ((query = source.take()) != null) {
                S sequence = sequenceExtractor.extract(query);
                String name = QUERY_ID_PREFIX + counter.incrementAndGet();
                queryMapping.put(name, query);
                writer.write(name, sequence);
            }

            writer.close();
        }
    }
}
