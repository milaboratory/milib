package com.milaboratory.core.mutations;

import com.milaboratory.core.sequence.Alphabet;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.util.BitArrayInt;
import com.milaboratory.util.IntArrayList;
import com.milaboratory.util.graph.AdjacencyMatrix;
import com.milaboratory.util.graph.IntAdjacencyMatrix;
import gnu.trove.impl.Constants;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
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
    static final double $_$_$ = 0XD_EP2F;//interesting number

    private final Alphabet<S> alphabet;
    private final AggregatedMutations<S>[] aggregators;
    private final int minimalPairCount;
    private final AggregatedMutations.MutationsFilter mutationsFilter;
    private final float mutationMatchWeight;
    private final float nullMutationMatchWeight;
    private final int minimalClonesInAllele;
    private final float minimalAssignmentScore;
    private final int depth;

    public VariantsAssembler(Alphabet<S> alphabet, AggregatedMutations<S>[] aggregators,
                             VariantsAssemblerParameters parameters) {
        this.alphabet = alphabet;
        this.aggregators = aggregators;
        this.minimalPairCount = parameters.getMinimalPairCount();
        this.mutationsFilter = parameters.getMutationsFilter();
        this.mutationMatchWeight = parameters.getMutationMatchWeight();
        this.nullMutationMatchWeight = parameters.getNullMutationMatchWeight();
        this.minimalClonesInAllele = parameters.getMinimalClonesInAllele();
        this.minimalAssignmentScore = parameters.getMinimalAssignmentScore();
        this.depth = parameters.getMaxDepth();
    }

    public static final int SHORT_ID_MASK = 0xFFFF;
    public static final int POSITION_OFFSET = 16;
    public static final int POSITION_MASK = 0xFFFF0000;

    @SuppressWarnings("unchecked")
    public AssignedVariants<S> findVariants() {
        AlleleAssignmentResult[] assignments = new AlleleAssignmentResult[aggregators.length];
        Arrays.fill(assignments, UNASSIGNED);

        final ArrayList<Wrapper<S>> oMutations = new ArrayList<>();
        int id = 0;
        for (int i = 0; i < depth; i++) {
            final IntermediateResult<S> temp = findVariants(assignments, oMutations.size(), i);
            oMutations.ensureCapacity(temp.mutations.size());
            for (int j = 0; j < temp.mutations.size(); ++j)
                oMutations.add(new Wrapper<>(temp.mutations.get(j), temp.counts[j], id++));
            if (temp.unassigned == 0 || temp.additionallyAssigned == 0)
                break;
        }
        final Wrapper[] wrappers = oMutations.toArray(new Wrapper[oMutations.size()]);
        Arrays.sort(wrappers);

        final TIntIntHashMap renaming = new TIntIntHashMap();
        final List<Mutations<S>> mutations = new ArrayList<>();
        final IntArrayList counts = new IntArrayList();
        id = 0;
        for (Wrapper wrapper : wrappers) {
            if (wrapper.mutations == null)
                continue;
            renaming.put(wrapper.id, id++);
            mutations.add(wrapper.mutations);
            counts.add(wrapper.count);
        }

        for (AlleleAssignmentResult assignment : assignments)
            if (assignment != null)
                assignment.alleleId = renaming.get(assignment.alleleId);

        return new AssignedVariants<>(mutations.toArray(new Mutations[mutations.size()]), counts.toArray(), assignments);
    }


    private IntermediateResult<S> findVariants(AlleleAssignmentResult[] assignments, int offset, int depth) {
        TObjectIntCustomHashMap<int[]> globalCounts = new TObjectIntCustomHashMap<>(IntArrayHashingStrategy,
                Constants.DEFAULT_CAPACITY,
                Constants.DEFAULT_LOAD_FACTOR, -1);
        int i;
        for (i = 0; i < aggregators.length; i++) {
            if (assignments[i] != UNASSIGNED)
                continue;
            List<int[]> filtered = aggregators[i].filtered(mutationsFilter);
            for (int[] muts : filtered)
                globalCounts.adjustOrPutValue(muts, 1, 0);
        }

        TObjectIntCustomHashMap<int[]> map = new TObjectIntCustomHashMap<>(IntArrayHashingStrategy,
                Constants.DEFAULT_CAPACITY,
                Constants.DEFAULT_LOAD_FACTOR, -1);
        final TIntObjectHashMap<int[]> iMap = new TIntObjectHashMap<>();
        int id, j;
        int[][] cloneMutationIds = new int[aggregators.length][];
        IntArrayList shortToFullId = new IntArrayList();
        for (i = 0; i < aggregators.length; i++) {
            if (assignments[i] != UNASSIGNED)
                continue;
            List<int[]> filtered = aggregators[i].filtered(mutationsFilter);
            int[] ids = new int[filtered.size()];
            j = 0;
            for (int[] muts : filtered) {
                if (globalCounts.get(muts) < minimalPairCount)
                    continue;
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

        if (map.size() == 0)
            return new IntermediateResult<>(Collections.EMPTY_LIST, new int[0], 0, 0);

        final IntAdjacencyMatrix intMatrix = new IntAdjacencyMatrix(map.size());
        for (int[] ids : cloneMutationIds) {
            if (ids == null)
                continue;
            for (i = 0; i < ids.length; ++i)
                for (j = i + 1; j < ids.length; ++j)
                    intMatrix.increment(ids[i] & SHORT_ID_MASK, ids[j] & SHORT_ID_MASK);
        }

        final AdjacencyMatrix matrix = intMatrix.filter(minimalPairCount);
        final List<BitArrayInt> cliques = matrix.calculateMaximalCliques();

        //identity mutation only on the first iteration
        final int[][] initialAlleles = new int[cliques.size() + (depth > 0 ? 0 : 1)][];

        for (i = 0; i < cliques.size(); i++) {
            initialAlleles[i] = cliques.get(i).getBits();
            for (j = 0; j < initialAlleles[i].length; j++)
                initialAlleles[i][j] = shortToFullId.get(initialAlleles[i][j]);
            Arrays.sort(initialAlleles[i]);
        }
        if (depth == 0)
            initialAlleles[initialAlleles.length - 1] = new int[0];

        final int[] allelesCounts = new int[initialAlleles.length];
        for (int k = 0; k < 3; ++k) {
            Arrays.fill(allelesCounts, 0);
            if (depth == 0)
                initialAlleles[initialAlleles.length - 1] = new int[0];

            final AlleleAssigner assigner = new AlleleAssigner(initialAlleles);
            for (i = 0; i < aggregators.length; i++) {
                if (assignments[i] == UNASSIGNED) {
                    final AlleleAssignmentResult r = assigner.assignAllele(cloneMutationIds[i]);
                    if (r.score >= minimalAssignmentScore)
                        ++allelesCounts[r.alleleId];
                }
            }

            for (i = 0; i < initialAlleles.length; i++)
                if (allelesCounts[i] < minimalClonesInAllele)
                    initialAlleles[i] = null;

        }

        Arrays.fill(allelesCounts, 0);
        int stillUnassigned = 0;
        final AlleleAssigner assigner = new AlleleAssigner(initialAlleles);
        for (i = 0; i < aggregators.length; i++) {
            if (assignments[i] == UNASSIGNED) {
                assignments[i] = assigner.assignAllele(cloneMutationIds[i]).shiftId(offset);
                assignments[i].depth = depth;
                if (assignments[i].score < minimalAssignmentScore)
                    assignments[i] = UNASSIGNED;
                else
                    ++allelesCounts[assignments[i].alleleId - offset];
            }
        }
        for (i = 0; i < aggregators.length; ++i) {
            if (assignments[i] != UNASSIGNED
                    && assignments[i].alleleId >= offset
                    && allelesCounts[assignments[i].alleleId - offset] < minimalClonesInAllele)
                assignments[i] = UNASSIGNED;
            if (assignments[i] == UNASSIGNED)
                ++stillUnassigned;
        }

        List<Mutations<S>> result = new ArrayList<>(initialAlleles.length);
        int additionallyAssigned = 0;
        for (i = 0; i < initialAlleles.length; i++)
            if (allelesCounts[i] < minimalClonesInAllele)
                result.add(null);
            else {
                result.add(toMutations(iMap, initialAlleles[i]));
                additionallyAssigned += allelesCounts[i];
            }

        return new IntermediateResult<>(result, allelesCounts, stillUnassigned, additionallyAssigned);
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
                if (allele != null)
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
                // Calculating matches
                int[] allele = alleles[i];
                if (allele == null)
                    continue;
                float score = 0.0f;
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
        public int alleleId;
        public double score;
        public int depth = 0;

        public AlleleAssignmentResult(int alleleId, double score) {
            this.alleleId = alleleId;
            this.score = score;
        }

        private AlleleAssignmentResult shiftId(int offset) {
            if (offset == 0)
                return this;
            alleleId += offset;
            return this;
        }

        @Override
        public String toString() {
            return "Allele(" + alleleId + "): score:" + score + "  depth: " + depth;
        }
    }

    private static final AlleleAssignmentResult UNASSIGNED = null;

    private static final class IntermediateResult<S extends Sequence<S>> {
        final List<Mutations<S>> mutations;
        final int[] counts;
        final int unassigned;
        final int additionallyAssigned;

        public IntermediateResult(List<Mutations<S>> mutations, int[] counts, int unassigned, int additionallyAssigned) {
            this.mutations = mutations;
            this.counts = counts;
            this.unassigned = unassigned;
            this.additionallyAssigned = additionallyAssigned;
        }
    }

    static Comparator<int[]> MI_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            return Integer.compare(getPosition(o1[0]), getPosition(o2[0]));
        }
    };

    private static final class Wrapper<S extends Sequence<S>> implements Comparable<Wrapper<S>> {
        final Mutations<S> mutations;
        final int count;
        final int id;

        public Wrapper(Mutations<S> mutations, int count, int id) {
            this.mutations = mutations;
            this.count = count;
            this.id = id;
        }

        @Override
        public int compareTo(Wrapper<S> o) {
            return Integer.compare(o.count, count);
        }
    }
}
