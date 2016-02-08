package com.milaboratory.core.mutations;

import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.mutations.generator.MutationModels;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.NucleotideMutationModel;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.test.TestUtil;
import com.milaboratory.util.BitArrayInt;
import com.milaboratory.util.IntArrayList;
import com.milaboratory.util.RandomUtil;
import com.milaboratory.util.graph.AdjacencyMatrix;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Test;

import java.util.*;

import static com.milaboratory.core.mutations.Mutation.getPosition;
import static com.milaboratory.core.mutations.MutationsCounter.IntArrayHashingStrategy;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class ConsesusTest {
    @Test
    public void test1() throws Exception {
        final int NUMBER_OF_CLONES = 100;
        final int MIN_NUMBER_OF_READS = 100;
        final int MAX_NUMBER_OF_READS = 1000;
        final int NUMBER_OF_ALLELES = 4;

        Well19937c random = RandomUtil.getThreadLocalRandom();
        random.setSeed(System.currentTimeMillis());

        NucleotideMutationModel model = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(10);
        NucleotideMutationModel noise = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(3);
        NucleotideSequence reference = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 100, 200);


        Mutations<NucleotideSequence>[] alleles = new Mutations[NUMBER_OF_ALLELES];
        NucleotideSequence[] alleleSeqs = new NucleotideSequence[NUMBER_OF_ALLELES];
        for (int i = 0; i < NUMBER_OF_ALLELES; i++) {
            alleles[i] = MutationsGenerator.generateMutations(reference, model);
            alleleSeqs[i] = alleles[i].mutate(reference);
        }


        List<AggregatedMutations<NucleotideSequence>> aggregators = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_CLONES; i++) {
            MutationConsensusBuilder<NucleotideSequence> builder =
                    new MutationConsensusBuilder<>(NucleotideSequence.ALPHABET, reference.size());

            int alleleId = random.nextInt(NUMBER_OF_ALLELES);
            Mutations<NucleotideSequence> allele = alleles[alleleId];
            NucleotideSequence alleleSeq = alleleSeqs[alleleId];

            int readsCount = MIN_NUMBER_OF_READS + random.nextInt(MAX_NUMBER_OF_READS - MIN_NUMBER_OF_READS);
            for (int j = 0; j < readsCount; j++) {
                Mutations<NucleotideSequence> noiseMuts =
                        MutationsGenerator.generateMutations(alleleSeq, noise);
                Mutations<NucleotideSequence> totalMutations = allele.combineWith(noiseMuts);
                Alignment<NucleotideSequence> alignment = new Alignment<>(reference, totalMutations, 0);
                builder.aggregate(alignment, Weight.ONE);
            }

            aggregators.add(builder.build());
        }

        TObjectIntCustomHashMap<int[]> map = new TObjectIntCustomHashMap<>(IntArrayHashingStrategy);
        TIntObjectHashMap<int[]> iMap = new TIntObjectHashMap();
        for (AggregatedMutations<NucleotideSequence> aggregator : aggregators)
            for (int[] muts : aggregator.mutations.valueCollection())
                if (!map.containsKey(muts)) {
                    iMap.put(map.size(), muts);
                    map.put(muts, map.size());
                }


        AdjacencyMatrix matrix = new AdjacencyMatrix(map.size());
        Decider<NucleotideSequence> decider = new Decider<NucleotideSequence>() {
            @Override
            public boolean connected(int p1, int p2,
                                     AggregatedMutations<NucleotideSequence> aggregatedMutations) {
                return true;
            }
        };

        IntArrayList mIds = new IntArrayList();
        IntArrayList mPositions = new IntArrayList();
        for (AggregatedMutations<NucleotideSequence> aggregator : aggregators) {
            mIds.clear(); mPositions.clear();
            TIntObjectIterator<int[]> it = aggregator.mutations.iterator();
            while (it.hasNext()) {
                it.advance();
                mIds.add(map.get(it.value()));
                mPositions.add(it.key());
            }
            for (int i = 0; i < mIds.size(); ++i) {
                for (int j = i + 1; j < mIds.size(); ++j) {
                    int p1 = mPositions.get(i), p2 = mPositions.get(j);
                    if (decider.connected(p1, p2, aggregator))
                        matrix.setConnected(mIds.get(i), mIds.get(j));
                }
            }
        }


        System.out.println("X");
        System.out.println(matrix);
        System.out.println("\n\n\n Alleles:");
        List<BitArrayInt> cliques = matrix.calculateMaximalCliques();

        for (BitArrayInt aa : cliques) {
            System.out.println(aa);
        }

        List<Mutations<NucleotideSequence>> actualAlleles = new ArrayList<>();

        for (BitArrayInt clique : cliques) {
            ArrayList<int[]> mutations = new ArrayList<>();
            for (int i = 0; i < clique.size(); i++)
                if (clique.get(i))
                    mutations.add(iMap.get(i));

            Collections.sort(mutations, MI_COMPARATOR);
            MutationsBuilder<NucleotideSequence> mBuilder = new MutationsBuilder<>(NucleotideSequence.ALPHABET);
            for (int[] ms : mutations)
                mBuilder.append(ms);
            actualAlleles.add(mBuilder.createAndDestroy());
        }

        Arrays.sort(alleles, M_COMPARATOR);
        Collections.sort(actualAlleles, M_COMPARATOR);

        System.out.println("\n\n\n Expected:");
        for (Mutations<NucleotideSequence> allele : alleles)
            System.out.println(allele);

        System.out.println("\n\n\n Actual:");
        for (Mutations<NucleotideSequence> allele : actualAlleles)
            System.out.println(allele);

        System.out.println("\n\n\n Eqs:");
        System.out.println("Same size: " + (alleles.length == actualAlleles.size()));
        for (int i = 0; i < alleles.length; i++)
            System.out.println(alleles[i].equals(actualAlleles.get(i)));
    }

    static Comparator<int[]> MI_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            return Integer.compare(getPosition(o1[0]), getPosition(o2[0]));
        }
    };

    static Comparator<Mutations<NucleotideSequence>> M_COMPARATOR = new Comparator<Mutations<NucleotideSequence>>() {
        @Override
        public int compare(Mutations<NucleotideSequence> o1, Mutations<NucleotideSequence> o2) {
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    };

    interface Decider<S extends Sequence<S>> {
        boolean connected(int position1, int position2, AggregatedMutations<S> aggregatedMutations);
    }
}
