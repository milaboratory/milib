package com.milaboratory.core.mutations;

import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.util.BitArrayInt;
import com.milaboratory.util.IntArrayList;
import com.milaboratory.util.graph.AdjacencyMatrix;
import com.milaboratory.util.graph.IntAdjacencyMatrix;
import gnu.trove.impl.Constants;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.milaboratory.core.mutations.Mutation.getPosition;
import static com.milaboratory.core.mutations.MutationsCounter.IntArrayHashingStrategy;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class VariantsAssembler<S extends Sequence<S>> {
    private final Alphabet<S> alphabet;
    private final List<AggregatedMutations<S>> aggregators;
    private final int minimalPairCount;
    private final AggregatedMutations.MutationsFilter mutationsFilter;

    public VariantsAssembler(Alphabet<S> alphabet, List<AggregatedMutations<S>> aggregators,
                             int minimalPairCount, AggregatedMutations.MutationsFilter mutationsFilter) {
        this.alphabet = alphabet;
        this.aggregators = aggregators;
        this.minimalPairCount = minimalPairCount;
        this.mutationsFilter = mutationsFilter;
    }

    public List<Mutations<S>> initialVariants() {
        TObjectIntCustomHashMap<int[]> map = new TObjectIntCustomHashMap<>(IntArrayHashingStrategy,
                Constants.DEFAULT_CAPACITY,
                Constants.DEFAULT_LOAD_FACTOR, -1);
        final TIntObjectHashMap<int[]> iMap = new TIntObjectHashMap<>();
        int id;
        for (AggregatedMutations<S> aggregator : aggregators) {
            for (int[] muts : aggregator.filtered(mutationsFilter)) {
                id = map.get(muts);
                if (id == -1) {
                    id = map.size();
                    iMap.put(id, muts); map.put(muts, id);
                }
            }
        }

        final IntAdjacencyMatrix intMatrix = new IntAdjacencyMatrix(map.size());
        final IntArrayList mIds = new IntArrayList();
        for (AggregatedMutations<S> aggregator : aggregators) {
            mIds.clear();
            for (int[] muts : aggregator.filtered(mutationsFilter))
                mIds.add(map.get(muts));
            for (int i = 0; i < mIds.size(); ++i)
                for (int j = i + 1; j < mIds.size(); ++j)
                    intMatrix.increment(mIds.get(i), mIds.get(j));
        }

        final AdjacencyMatrix matrix = intMatrix.filter(minimalPairCount);
        final List<BitArrayInt> cliques = matrix.calculateMaximalCliques();
        final List<Mutations<S>> initialAlleles = new ArrayList<>();
        for (BitArrayInt clique : cliques) {
            final ArrayList<int[]> mutations = new ArrayList<>();
            for (int i = 0; i < clique.size(); i++)
                if (clique.get(i))
                    mutations.add(iMap.get(i));
            Collections.sort(mutations, MI_COMPARATOR);
            MutationsBuilder<S> mBuilder = new MutationsBuilder<>(alphabet);
            for (int[] ms : mutations)
                mBuilder.append(ms);
            initialAlleles.add(mBuilder.createAndDestroy());
        }

        return initialAlleles;
    }

    static Comparator<int[]> MI_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            return Integer.compare(getPosition(o1[0]), getPosition(o2[0]));
        }
    };

}
