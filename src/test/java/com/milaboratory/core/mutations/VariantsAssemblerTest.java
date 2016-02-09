package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.mutations.generator.MutationModels;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.NucleotideMutationModel;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.core.sequence.Sequence;
import com.milaboratory.test.TestUtil;
import com.milaboratory.util.RandomUtil;
import org.apache.commons.math3.random.Well19937c;
import org.junit.Test;

import java.util.*;

import static com.milaboratory.core.mutations.Mutation.getPosition;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public class VariantsAssemblerTest {
    @Test
    public void test1() throws Exception {
        final int NUMBER_OF_CLONES = 400;
        final int MIN_NUMBER_OF_READS = 100;
        final int MAX_NUMBER_OF_READS = 1000;
        final int MIN_REF_LENGTH = 100;
        final int MAX_REF_LENGTH = 200;
        final double RANDOM_COVERAGE_OFFSET_SIZE = 0.2;
        final int NUMBER_OF_ALLELES = 4;
        final boolean FIRST_ALLELE_IS_WT = true;

        RandomUtil.reseedThreadLocalFromTime();
        //RandomUtil.reseedThreadLocal(1234);
        Well19937c random = RandomUtil.getThreadLocalRandom();

        NucleotideMutationModel model = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(30);
        model.reseed(random.nextLong());
        NucleotideMutationModel noise = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(3);
        noise.reseed(random.nextLong());

        NucleotideSequence reference = TestUtil.randomSequence(NucleotideSequence.ALPHABET, MIN_REF_LENGTH, MAX_REF_LENGTH);
        final int randomCoverageOffsetSize = (int) (RANDOM_COVERAGE_OFFSET_SIZE * reference.size());

        Mutations<NucleotideSequence>[] alleles = new Mutations[NUMBER_OF_ALLELES];
        NucleotideSequence[] alleleSeqs = new NucleotideSequence[NUMBER_OF_ALLELES];
        for (int i = FIRST_ALLELE_IS_WT ? 1 : 0; i < NUMBER_OF_ALLELES; i++) {
            alleles[i] = MutationsGenerator.generateMutations(reference, model);
            alleleSeqs[i] = alleles[i].mutate(reference);
        }
        if (FIRST_ALLELE_IS_WT) {
            alleles[0] = Mutations.EMPTY_NUCLEOTIDE_MUTATIONS;
            alleleSeqs[0] = reference;
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
                Range r = new Range(random.nextInt(randomCoverageOffsetSize), reference.size() - random.nextInt(randomCoverageOffsetSize));
                totalMutations = totalMutations.extractMutationsForRange(r).move(r.getFrom());
                Alignment<NucleotideSequence> alignment = new Alignment<>(reference, totalMutations, r, 0);
                builder.aggregate(alignment, Weight.ONE);
            }

            aggregators.add(builder.build());
        }

        VariantsAssembler<NucleotideSequence> assembler = new VariantsAssembler<>(NucleotideSequence.ALPHABET,
                aggregators, 50, new AggregatedMutations.MutationsFilter() {
            @Override
            public boolean filter(int position, int[] mutations, long coverageWeight, long mutationWeight) {
                return 1. * mutationWeight / coverageWeight > .7;
            }
        });

        //Arrays.sort(alleles, M_COMPARATOR);
        List<Mutations<NucleotideSequence>> actualAlleles = assembler.initialVariants();
        //Collections.sort(actualAlleles, M_COMPARATOR);
        HashSet<Mutations<NucleotideSequence>> expectedSet = new HashSet<>(Arrays.asList(alleles));

        System.out.println("\nUnique actual:");
        for (Mutations<NucleotideSequence> actualAllele : actualAlleles)
            if (!expectedSet.remove(actualAllele))
                System.out.println(actualAllele);
        System.out.println("\nUnique expected:");
        for (Mutations<NucleotideSequence> expected : expectedSet)
            System.out.println(expected);

//
//        System.out.println("\n\n\n Expected:");
//        for (Mutations<NucleotideSequence> allele : alleles)
//            System.out.println(allele);
//
//        System.out.println("\n\n\n Actual:");
//        for (Mutations<NucleotideSequence> allele : actualAlleles)
//            System.out.println(allele);
//
//        System.out.println("\n\n\n Eqs:");
//        System.out.println("Same size: " + (alleles.length == actualAlleles.size()));
//        for (int i = 0; i < alleles.length; i++)
//            System.out.println(alleles[i].equals(actualAlleles.get(i)));
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
