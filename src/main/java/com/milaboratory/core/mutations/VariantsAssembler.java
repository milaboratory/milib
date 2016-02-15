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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

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
    private final float mutationMatchWeight;
    private final float nullMutationMatchWeight;
    private final int minimalClonesInAllele;
    private final float minimalAssignmentScore;

    public VariantsAssembler(Alphabet<S> alphabet, List<AggregatedMutations<S>> aggregators,
                             VariantsAssemblerParameters parameters) {
        this.alphabet = alphabet;
        this.aggregators = aggregators;
        this.minimalPairCount = parameters.getMinimalPairCount();
        this.mutationsFilter = parameters.getMutationsFilter();
        this.mutationMatchWeight = parameters.getMutationMatchWeight();
        this.nullMutationMatchWeight = parameters.getNullMutationMatchWeight();
        this.minimalClonesInAllele = parameters.getMinimalClonesInAllele();
        this.minimalAssignmentScore = parameters.getMinimalAssignmentScore();
    }

    public static final int SHORT_ID_MASK = 0xFFFF;
    public static final int POSITION_OFFSET = 16;
    public static final int POSITION_MASK = 0xFFFF0000;

    public AssignedVariants<S> initialVariants() {
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

        final int[][] initialAlleles = new int[cliques.size() + 1][];

        for (i = 0; i < cliques.size(); i++) {
            initialAlleles[i] = cliques.get(i).getBits();
            for (j = 0; j < initialAlleles[i].length; j++)
                initialAlleles[i][j] = shortToFullId.get(initialAlleles[i][j]);
            Arrays.sort(initialAlleles[i]);
        }
        initialAlleles[initialAlleles.length - 1] = new int[0];

        final AlleleAssigner assigner = new AlleleAssigner(initialAlleles);
        final AlleleAssignmentResult[] assignments = new AlleleAssignmentResult[aggregators.size()];
        final int[] allelesCounts = new int[initialAlleles.length];

        for (i = 0; i < cloneMutationIds.length; i++) {
            assignments[i] = assigner.assignAllele(cloneMutationIds[i]);
            if (assignments[i].score < minimalAssignmentScore)
                assignments[i] = UNASSIGNED;
            else
                ++allelesCounts[assignments[i].alleleId];
        }

        for (i = 0; i < cloneMutationIds.length; i++)
            if (assignments[i] != UNASSIGNED && allelesCounts[assignments[i].alleleId] < minimalClonesInAllele)
                assignments[i] = UNASSIGNED;

        Mutations<S>[] result = new Mutations[initialAlleles.length];
        for (i = 0; i < initialAlleles.length; i++)
            if (allelesCounts[i] < minimalClonesInAllele)
                result[i] = null;
            else
                result[i] = toMutations(iMap, initialAlleles[i]);


        return new AssignedVariants<>(result, assignments);
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

        public AlleleAssignmentResult assignAllele(int[] clone) {
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

                final float length = allele.length * mutationMatchWeight + (positionsOfInterest.length - allele.length) * nullMutationMatchWeight;
                if (length == 0f)
                    score = 1.0f;
                else
                    score /= length;

                if (bestId == -1 || score > bestScore) {
                    bestId = i;
                    bestScore = score;
                }
            }
            return new AlleleAssignmentResult(bestId, bestScore);
        }
    }

    public final static class AlleleAssignmentResult {
        final int alleleId;
        final double score;

        public AlleleAssignmentResult(int alleleId, double score) {
            this.alleleId = alleleId;
            this.score = score;
        }

        @Override
        public String toString() {
            return "Allele(" + alleleId + "): " + score;
        }
    }

    static final AlleleAssignmentResult UNASSIGNED = null;

    static Comparator<int[]> MI_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            return Integer.compare(getPosition(o1[0]), getPosition(o2[0]));
        }
    };

}
