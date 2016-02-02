package com.milaboratory.core.mutations;

import com.milaboratory.core.alignment.AffineGapAlignmentScoring;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.mutations.generator.MutationModels;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.NucleotideMutationModel;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.test.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public class MutationConsensusBuilderTest {
    @Test
    public void test1() throws Exception {
        NucleotideMutationModel coreModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(30);
        NucleotideMutationModel additionalModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(3);
        NucleotideSequence coreSequence = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 100, 200);
        Mutations<NucleotideSequence> coreMutations = MutationsGenerator.generateMutations(coreSequence, coreModel);
        NucleotideSequence mutatedCoreSequence = coreMutations.mutate(coreSequence);

        MutationConsensusBuilder<NucleotideSequence> builder = new MutationConsensusBuilder<>(
                NucleotideSequence.ALPHABET, coreSequence.size());
        for (int i = 0; i < 1000; i++) {
            Mutations<NucleotideSequence> additionalMutations =
                    MutationsGenerator.generateMutations(mutatedCoreSequence, additionalModel);
            Mutations<NucleotideSequence> totalMutations = coreMutations.combineWith(additionalMutations);
            Alignment<NucleotideSequence> alignment = new Alignment<>(coreSequence, totalMutations, 0);
            builder.aggregate(alignment, Weight.ONE);
        }

        AggregatedMutations<NucleotideSequence> result = builder.build();

        AggregatedMutations.Consensus<NucleotideSequence> consesus = result.buildAlignments(coreSequence, new AggregatedMutations.QualityProvider() {
            @Override
            public byte getQuality(long coverageWeight, long mutationCount, int[] mutations) {
                return 10;
            }
        }, AffineGapAlignmentScoring.IGBLAST_NUCLEOTIDE_SCORING);

        Assert.assertEquals(coreMutations, consesus.alignment.getAbsoluteMutations());
    }

    @Test
    public void test2() throws Exception {
        NucleotideSequence seq = new NucleotideSequence("ATTAGACA");

        MutationConsensusBuilder<NucleotideSequence> builder = new MutationConsensusBuilder<>(
                NucleotideSequence.ALPHABET, seq.size());

        Mutations<NucleotideSequence> mutations = Mutations.decodeNuc("I8G");

        builder.aggregate(new Alignment<>(seq, mutations, 0), Weight.ONE);

        AggregatedMutations<NucleotideSequence> result = builder.build();

        AggregatedMutations.Consensus<NucleotideSequence> consensus = result.buildAlignments(seq,
                new AggregatedMutations.QualityProvider() {
                    @Override
                    public byte getQuality(long coverageWeight, long mutationCount, int[] mutations) {
                        return 10;
                    }
                }, AffineGapAlignmentScoring.IGBLAST_NUCLEOTIDE_SCORING);

        Assert.assertEquals(mutations, consensus.alignment.getAbsoluteMutations());
    }
}