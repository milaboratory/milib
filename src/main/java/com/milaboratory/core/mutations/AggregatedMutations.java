package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.core.sequence.SequenceQuality;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Collections;
import java.util.List;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class AggregatedMutations<S extends Sequence<S>> {
    final Alphabet<S> alphabet;
    final CoverageCounter coverageCounter;
    final CoverageCounter mutationWeight;
    final TIntObjectHashMap<int[]> mutations;
    final Range range;

    public AggregatedMutations(Alphabet<S> alphabet, CoverageCounter coverageCounter, CoverageCounter mutationWeight,
                               TIntObjectHashMap<int[]> mutations, Range range) {
        this.alphabet = alphabet;
        this.coverageCounter = coverageCounter;
        this.mutationWeight = mutationWeight;
        this.mutations = mutations;
        this.range = range;
    }

    public long coverageWeight(int position) {
        return coverageCounter.totalWeight(position);
    }

    public long mutationWeight(int position) {
        return mutationWeight.totalWeight(position);
    }

    public Mutations<S> mutation(int position) {
        int[] mutations = this.mutations.get(position);
        if (mutations == null)
            return Mutations.empty(alphabet);
        return new Mutations<>(alphabet, mutations, true);
    }

    public List<Consensus<S>> buildAlignments() {
        final int from = range.getFrom(), to = range.getTo(), length = range.length();
        for (int i = 0; i < length; ++i) {

        }

        return Collections.emptyList();
    }

    public static final class Consensus<S extends Sequence<S>> {
        public final SequenceQuality quality;
        public final Sequence<S> sequence;
        public final Alignment<S> alignment;

        public Consensus(SequenceQuality quality, Sequence<S> sequence, Alignment<S> alignment) {
            this.quality = quality;
            this.sequence = sequence;
            this.alignment = alignment;
        }
    }
}
