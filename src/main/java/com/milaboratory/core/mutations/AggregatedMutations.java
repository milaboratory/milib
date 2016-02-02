package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.alignment.AlignmentScoring;
import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.core.sequence.SequenceQuality;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class AggregatedMutations<S extends Sequence<S>> {
    final Alphabet<S> alphabet;
    final CoverageCounter coverageWeight;
    final CoverageCounter mutationWeight;
    final TIntObjectHashMap<int[]> mutations;
    final Range range;

    public AggregatedMutations(Alphabet<S> alphabet, CoverageCounter coverageWeight, CoverageCounter mutationWeight,
                               TIntObjectHashMap<int[]> mutations, Range range) {
        this.alphabet = alphabet;
        this.coverageWeight = coverageWeight;
        this.mutationWeight = mutationWeight;
        this.mutations = mutations;
        this.range = range;
    }

    public long coverageWeight(int position) {
        return coverageWeight.totalWeight(position);
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

    public Consensus<S> buildAlignments(final S reference,
                                        final QualityProvider qualityProvider,
                                        final AlignmentScoring<S> scoring) {
        final MutationsBuilder<S> mBuilder = new MutationsBuilder<>(alphabet);
        final int from = range.getFrom(), to = range.getTo(), length = range.length();

        int mutationsDelta = 0;
        for (int[] ints : mutations.valueCollection())
            mutationsDelta += MutationsUtil.getLengthDelta(ints);

        final byte[] quality = new byte[length + mutationsDelta];
        Arrays.fill(quality, Byte.MAX_VALUE);

        mutationsDelta = 0;
        for (int position = from; position < to; ++position) {
            int[] muts = mutations.get(position);
            byte q = qualityProvider.getQuality(coverageWeight(position), mutationWeight(position), null);
            int index = mutationsDelta + position - from;
            if (muts != null) {
                int lDelta = MutationsUtil.getLengthDelta(muts);
                if (lDelta < 0) {
                    assert lDelta == -1;
                    if (index - 1 >= 0)
                        quality[index - 1] = min(quality[mutationsDelta + position - 1], q);
                    if (index < quality.length)
                        quality[index] = min(quality[mutationsDelta + position], q);
                } else
                    for (int i = 0; i < lDelta + 1; i++)
                        quality[index + i] = q;

                mBuilder.append(muts);
                mutationsDelta += lDelta;
            } else if (index > 0 && index < quality.length)
                quality[index] = q;
        }

        return new Consensus<>(new SequenceQuality(quality), reference,
                new Alignment<>(reference, mBuilder.createAndDestroy(), range, scoring));
    }

    public static byte min(byte a, byte b) {
        return (a <= b) ? a : b;
    }

    public interface QualityProvider {
        byte getQuality(long coverageWeight, long mutationCount, int[] mutations);
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

        //public ArrayList<Co>
    }
}
