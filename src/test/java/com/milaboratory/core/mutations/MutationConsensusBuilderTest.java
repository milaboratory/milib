package com.milaboratory.core.mutations;

import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.mutations.generator.MutationModels;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.NucleotideMutationModel;
import com.milaboratory.core.sequence.NucleotideSequence;
import com.milaboratory.test.TestUtil;
import org.junit.Test;

public class MutationConsensusBuilderTest {
    @Test
    public void test1() throws Exception {
        NucleotideMutationModel coreModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(3);
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
        //List<AggregatedMutations.Consensus<NucleotideSequence>> consensus = result.buildAlignments();
    }
}