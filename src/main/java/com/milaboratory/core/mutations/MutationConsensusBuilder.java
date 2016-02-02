package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class MutationConsensusBuilder<S extends Sequence<S>> {
    public final CoverageCounter coverage;
    public final MutationsCounter mutations;
    public final Alphabet<S> alphabet;
    public final Range range;

    public MutationConsensusBuilder(Alphabet<S> alphabet, int sequenceLength) {
        this(alphabet, new Range(0, sequenceLength));
    }

    public MutationConsensusBuilder(Alphabet<S> alphabet, Range seqRange) {
        this.alphabet = alphabet;
        this.coverage = new CoverageCounter(seqRange);
        this.mutations = new MutationsCounter();
        this.range = seqRange;
    }

    public void aggregate(Alignment<S> alignment, Weight weight) {
        coverage.aggregate(alignment.getSequence1Range(), weight);
        MutationsEnumerator enumerator = new MutationsEnumerator(alignment.getAbsoluteMutations());
        while (enumerator.next())
            mutations.adjust(alignment.getAbsoluteMutations(), enumerator, weight);
    }

    public AggregatedMutations<S> build() {
        final int refFrom = range.getFrom();
        final int refTo = range.getTo();
        final int len = range.length();

        final long[] topMutationCount = new long[len];
        final long[] totalMutationCount = new long[len];
        final TIntObjectHashMap<int[]> def = new TIntObjectHashMap<>();

        final TIntLongIterator it = mutations.counter.iterator();
        while (it.hasNext()) {
            it.advance();
            int mutation = it.key();
            long count = it.value();
            if ((mutation & Mutation.MUTATION_TYPE_MASK) != 0) {
                int position = Mutation.getPosition(mutation);
                int index = position - refFrom;
                if (topMutationCount[index] < count) {
                    topMutationCount[index] = count;
                    def.put(position, new int[]{mutation});
                }
                totalMutationCount[index] += count;
            }
        }

        if (mutations.insertMapping != null) {
            TObjectIntIterator<int[]> itO = mutations.insertMapping.iterator();
            while (itO.hasNext()) {
                itO.advance();
                int[] key = itO.key();
                int position = Mutation.getPosition(key[0]);
                long count = mutations.counter.get(itO.value());
                int index = position - refFrom;
                if (topMutationCount[index] < count) {
                    topMutationCount[index] = count;
                    def.put(position, key);
                }
                totalMutationCount[index] += count;
            }
        }

        for (int position = refFrom; position < refTo; ++position) {
            long noMutationCount = coverage.totalWeight(position) - totalMutationCount[position - refFrom];
            if (noMutationCount > topMutationCount[position - refFrom]) {
                topMutationCount[position - refFrom] = noMutationCount;
                def.remove(position);
            }
        }

        return new AggregatedMutations<>(alphabet, coverage,
                new CoverageCounter(refFrom, refTo, topMutationCount), def, range);
    }
}
