package com.milaboratory.core.mutations;

import com.milaboratory.core.alignment.AffineGapAlignmentScoring;
import com.milaboratory.core.alignment.Alignment;
import com.milaboratory.core.alignment.AlignmentUtils;
import com.milaboratory.core.mutations.generator.MutationModels;
import com.milaboratory.core.mutations.generator.MutationsGenerator;
import com.milaboratory.core.mutations.generator.NucleotideMutationModel;
import com.milaboratory.core.sequence.NSequenceWithQuality;
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

        AffineGapAlignmentScoring<NucleotideSequence> sc = AffineGapAlignmentScoring.IGBLAST_NUCLEOTIDE_SCORING;
        AggregatedMutations.Consensus<NucleotideSequence> consesus = result.buildAlignments(coreSequence, new AggregatedMutations.QualityProvider() {
            @Override
            public byte getQuality(long coverageWeight, long mutationCount, int[] mutations) {
                return 10;
            }
        }, sc);

        Assert.assertEquals(coreMutations, consesus.alignment.getAbsoluteMutations());

        System.out.println(consesus.alignment.getAlignmentHelper().toStringWithSeq2Quality(consesus.quality));
        System.out.println(consesus.quality);

        ArrayList<AggregatedMutations.Consensus<NucleotideSequence>> split = consesus.split((byte) 0, sc);
        Assert.assertEquals(split.size(), 1);
        Assert.assertEquals(split.get(0).alignment, consesus.alignment);

        split = consesus.split((byte) 127, sc);
        Assert.assertEquals(split.size(), 0);
    }

    @Test
    public void test3() throws Exception {
        RandomUtil.reseedThreadLocalFromTime();
        NucleotideMutationModel coreModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(10);
        NucleotideMutationModel additionalModel = MutationModels.getEmpiricalNucleotideMutationModel().multiplyProbabilities(3);
        NucleotideSequence coreSequence = TestUtil.randomSequence(NucleotideSequence.ALPHABET, 100, 200);

        Mutations<NucleotideSequence> coreMutations1 = MutationsGenerator.generateMutations(coreSequence, coreModel);
        NucleotideSequence mutatedCoreSequence1 = coreMutations1.mutate(coreSequence);
        Alignment<NucleotideSequence> coreAlignment1 = new Alignment<>(coreSequence, coreMutations1, 0);

        Mutations<NucleotideSequence> coreMutations2 = MutationsGenerator.generateMutations(mutatedCoreSequence1, coreModel);
        coreMutations2 = coreMutations1.combineWith(coreMutations2);
        NucleotideSequence mutatedCoreSequence2 = coreMutations2.mutate(coreSequence);
        Alignment<NucleotideSequence> coreAlignment2 = new Alignment<>(coreSequence, coreMutations2, 0);

        MutationConsensusBuilder<NucleotideSequence> builder = new MutationConsensusBuilder<>(
                NucleotideSequence.ALPHABET, coreSequence.size());

        for (int i = 0; i < 1000; i++) {
            boolean b = RandomUtil.getThreadLocalRandom().nextBoolean();
            NucleotideSequence seq = b ? mutatedCoreSequence1 : mutatedCoreSequence2;
            Mutations<NucleotideSequence> muts = b ? coreMutations1 : coreMutations2;
            Mutations<NucleotideSequence> additionalMutations =
                    MutationsGenerator.generateMutations(seq, additionalModel);
            Mutations<NucleotideSequence> totalMutations = muts.combineWith(additionalMutations);
            Alignment<NucleotideSequence> alignment = new Alignment<>(coreSequence, totalMutations, 0);
            builder.aggregate(alignment, Weight.ONE);
        }

        AggregatedMutations<NucleotideSequence> result = builder.build();

        AffineGapAlignmentScoring<NucleotideSequence> sc = AffineGapAlignmentScoring.IGBLAST_NUCLEOTIDE_SCORING;
        AggregatedMutations.Consensus<NucleotideSequence> consesus = result.buildAlignments(coreSequence, new AggregatedMutations.QualityProvider() {
            @Override
            public byte getQuality(long coverageWeight, long mutationCount, int[] mutations) {
                return (byte)(40 * mutationCount / coverageWeight);
            }
        }, sc);

        //Assert.assertEquals(consesus.sequence.size(), consesus.quality.size());
        System.out.println(coreAlignment1.getAlignmentHelper());
        System.out.println();
        System.out.println(coreAlignment2.getAlignmentHelper());
        System.out.println();

        System.out.println(consesus.alignment.getAlignmentHelper().toStringWithSeq2Quality(consesus.quality));
        System.out.println();
        System.out.println(new NSequenceWithQuality(AlignmentUtils.getAlignedSequence2Part(consesus.alignment), consesus.quality).toPrettyString());
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