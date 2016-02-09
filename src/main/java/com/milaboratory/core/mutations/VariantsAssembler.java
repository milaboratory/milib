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
import gnu.trove.set.hash.TIntHashSet;

import java.util.*;

import static com.milaboratory.core.mutations.Mutation.MUTATION_POSITION_MASK;
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
    private final float mutationMatchWeight = 10.0f;
    private final float nullMutationMatchWeight = 1.0f;

    public VariantsAssembler(Alphabet<S> alphabet, List<AggregatedMutations<S>> aggregators,
                             int minimalPairCount, AggregatedMutations.MutationsFilter mutationsFilter) {
        this.alphabet = alphabet;
        this.aggregators = aggregators;
        this.minimalPairCount = minimalPairCount;
        this.mutationsFilter = mutationsFilter;
    }

    public static final int SHORT_ID_MASK = 0xFFFF;
    public static final int POSITION_OFFSET = 16;
    public static final int POSITION_MASK = 0xFFFF0000;

    public List<Mutations<S>> initialVariants() {
        TObjectIntCustomHashMap<int[]> map = new TObjectIntCustomHashMap<>(IntArrayHashingStrategy,
                Constants.DEFAULT_CAPACITY,
                Constants.DEFAULT_LOAD_FACTOR, -1);
        final TIntObjectHashMap<int[]> iMap = new TIntObjectHashMap<>();
        int id, j, i;
        int[][] cloneMutationIds = new int[aggregators.size()][];
        IntArrayList shortToFullId = new IntArrayList();
        for (i = 0; i < aggregators.size(); i++) {
            List<int[]> filtered = aggregators.get(i).filtered(mutationsFilter);
            int[] ids = new int[filtered.size()];
            j = 0;
            for (int[] muts : filtered) {
                id = map.get(muts);
                if (id == -1) {
                    id = map.size();
                    id |= (Mutation.getPosition(muts[0]) << POSITION_OFFSET);
                    iMap.put(id, muts);
                    map.put(muts, id);
                    assert shortToFullId.size() == (id & SHORT_ID_MASK);
                    shortToFullId.add(id);
                }
                ids[j++] = id;
            }
            Arrays.sort(ids);
            cloneMutationIds[i] = ids;
        }

        final IntAdjacencyMatrix intMatrix = new IntAdjacencyMatrix(map.size());
        for (int[] ids : cloneMutationIds)
            for (i = 0; i < ids.length; ++i)
                for (j = i + 1; j < ids.length; ++j)
                    intMatrix.increment(ids[i] & SHORT_ID_MASK, ids[j] & SHORT_ID_MASK);

        final AdjacencyMatrix matrix = intMatrix.filter(minimalPairCount);
        final List<BitArrayInt> cliques = matrix.calculateMaximalCliques();

        final int[][] initialAlleles = new int[cliques.size()][];

        for (i = 0; i < cliques.size(); i++) {
            initialAlleles[i] = cliques.get(i).getBits();
            for (j = 0; j < initialAlleles[i].length; j++)
                initialAlleles[i][j] = shortToFullId.get(initialAlleles[i][j]);
            Arrays.sort(initialAlleles[i]);
        }

        i = 0;
        for (int[] initialAllele : initialAlleles)
            System.out.println("A" + (i++) + ": " + toMutations(iMap, initialAllele));

        AlleleAssigner assigner = new AlleleAssigner(initialAlleles);

        AlleleAssignmentResult aResult = new AlleleAssignmentResult();
        for (int[] clone : cloneMutationIds) {
            assigner.assignAllele(clone, aResult);
            System.out.println(aResult);
        }

        List<Mutations<S>> result = new ArrayList<>();
        for (int[] initialAllele : initialAlleles)
            result.add(toMutations(iMap, initialAllele));

        return result;
    }

    Mutations<S> toMutations(final TIntObjectHashMap<int[]> iMap, final int[] mutIds) {
        int[][] muts = new int[mutIds.length][];
        int size = 0;
        for (int i = 0; i < mutIds.length; i++)
            size += (muts[i] = iMap.get(mutIds[i])).length;

        Arrays.sort(muts, MI_COMPARATOR);

        MutationsBuilder<S> mBuilder = new MutationsBuilder<>(alphabet)
                .ensureCapacity(size);

        for (int[] ms : muts)
            mBuilder.append(ms);

        return mBuilder.createAndDestroy();
    }

    final class AlleleAssigner {
        final int[][] alleles;
        final int[] positionsOfInterest;

        public AlleleAssigner(int[][] alleles) {
            this.alleles = alleles;
            final TIntHashSet positions = new TIntHashSet();
            for (int[] allele : alleles)
                for (int mutId : allele)
                    // Leave positions are left shifted (see assignAllele for usage)
                    positions.add(mutId & MUTATION_POSITION_MASK);
            this.positionsOfInterest = positions.toArray();
            Arrays.sort(this.positionsOfInterest);
        }

        public void assignAllele(int[] clone, AlleleAssignmentResult result) {
            //int[] match = new int[alleles.length];
            int bestId = -1;
            float bestScore = 0.0f;
            for (int i = 0; i < alleles.length; i++) {
                float score = 0.0f;

                // Calculating matches
                int[] allele = alleles[i];
                int aPointer = 0, cPointer = 0;
                int inAllele, inClone, tmp;
                for (int position : positionsOfInterest) {
                    // Position is already shifted, the same as in ids

                    // -1 = no match found
                    inAllele = -1;
                    while (aPointer < allele.length && (tmp = allele[aPointer]) <= (position | 0xFFFF)) {
                        ++aPointer;
                        // If allele contains mutation in this position save it to inAllele
                        if ((tmp & POSITION_MASK) == position) {
                            inAllele = tmp;
                            break;
                        }
                    }

                    // If there will be no mutation with the same position in clone array,
                    // it is a match
                    inClone = -1;
                    while (cPointer < clone.length && (tmp = clone[cPointer]) <= (position | 0xFFFF)) {
                        cPointer++;
                        if ((tmp & POSITION_MASK) == position) {
                            // Found mutation with the same position
                            inClone = tmp;
                            break;
                        }
                    }

                    // This position is in the same state in clone and allele
                    if (inClone == inAllele) {
                        if (inAllele == -1)
                            score += nullMutationMatchWeight;
                        else
                            score += mutationMatchWeight;
                    } else {
                        if (inAllele == -1)
                            score -= nullMutationMatchWeight;
                        else
                            score -= mutationMatchWeight;
                    }
                }

                score /= allele.length * mutationMatchWeight + (positionsOfInterest.length - allele.length) * nullMutationMatchWeight;

                if (bestId == -1 || score > bestScore) {
                    bestId = i;
                    bestScore = score;
                }
            }
            result.set(bestId, bestScore);
        }
    }

    final class AlleleAssignmentResult {
        int alleleId;
        double score;

        public void set(int alleleId, double score) {
            this.alleleId = alleleId;
            this.score = score;
        }

        @Override
        public String toString() {
            return "A" + alleleId + ": " + score;
        }
    }

    static Comparator<int[]> MI_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            return Integer.compare(getPosition(o1[0]), getPosition(o2[0]));
        }
    };

}
