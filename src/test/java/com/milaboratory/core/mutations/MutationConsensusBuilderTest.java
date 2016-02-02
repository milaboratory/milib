package com.milaboratory.core.mutations;

import com.milaboratory.core.alignment.AffineGapAlignmentScoring;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.mutations.generator.MutationModels;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.NucleotideMutationModel;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.test.TestUtil;
import com.milaboratory.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class MutationConsensusBuilderTest {
    @Test
    public void test1() throws Exception {
        RandomUtil.getThreadLocalRandom().setSeed(12345);
        NucleotideMutationModel coreModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(30);
        NucleotideMutationModel additionalModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(3);
        NucleotideSequence coreSequence = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 100, 200);
        Mutations<NucleotideSequence> coreMutations = MutationsGenerator.generateMutations(coreSequence, coreModel,
                0, coreSequence.size() - 1);
        NucleotideSequence mutatedCoreSequence = coreMutations.mutate(coreSequence);

        MutationConsensusBuilder<NucleotideSequence> builder = new MutationConsensusBuilder<>(
                NucleotideSequence.ALPHABET, coreSequence.size());
        for (int i = 0; i < 1000; i++) {
            Mutations<NucleotideSequence> additionalMutations =
                    MutationsGenerator.generateMutations(mutatedCoreSequence, additionalModel, 0,
                            mutatedCoreSequence.size() - 1);
            Mutations<NucleotideSequence> totalMutations = coreMutations.combineWith(additionalMutations);
            Alignment<NucleotideSequence> alignment = new Alignment<>(coreSequence, totalMutations, 0);
            builder.aggregate(alignment, Weight.ONE);
        }

        AggregatedMutations<NucleotideSequence> result = builder.build();

        AffineGapAlignmentScoring<NucleotideSequence> sc = AffineGapAlignmentScoring.IGBLAST_NUCLEOTIDE_SCORING;
        AggregatedMutations.Consensus<NucleotideSequence> consesus = result.buildAlignments(coreSequence, new AggregatedMutations.QualityProvider() {
            @Override
            public byte getQuality(long coverageWeight, long mutationCount, int[] mutations) {
                return 10;
            }
        }, sc);

        ArrayList<AggregatedMutations.Consensus<NucleotideSequence>> split = consesus.split((byte) 127, sc);
        for (AggregatedMutations.Consensus<NucleotideSequence> qs : split) {
            System.out.println(qs.alignment.getAlignmentHelper());
        }

        Assert.assertEquals(coreMutations, consesus.alignment.getAbsoluteMutations());
    }
}