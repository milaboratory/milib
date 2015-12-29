//package com.milaboratory.core.alignment;
//
//import com.milaboratory.core.sequence.Alphabet;
//import com.milaboratory.core.sequence.NucleotideSequence;
//import com.milaboratory.core.sequence.Sequence;
//
///**
// * @author Dmitry Bolotin
// * @author Stanislav Poslavsky
// */
//public class MixedScoring<S extends Sequence<S>> extends AbstractAlignmentScoring<S>
//        implements java.io.Serializable {
//    /**
//     * Penalty for gap, must be < 0
//     */
//    private final int linearGapPenalty,
//            affineGapOpenPenalty,
//            affineGapExtensionPenalty;
//
//    /**
//     * Creates new LinearGapAlignmentScoring. Required for deserialization defaults.
//     */
//    @SuppressWarnings("unchecked")
//    private MixedScoring() {
//        super((Alphabet) NucleotideSequence.ALPHABET);
//        linearGapPenalty = -5;
//        affineGapOpenPenalty = -5;
//        affineGapExtensionPenalty = -1;
//    }
//
//    public MixedScoring(Alphabet<S> alphabet, int[] subsMatrix,
//                        int linearGapPenalty,
//                        int affineGapOpenPenalty,
//                        int affineGapExtensionPenalty) {
//        super(alphabet, subsMatrix);
//        this.linearGapPenalty = linearGapPenalty;
//        this.affineGapOpenPenalty = affineGapOpenPenalty;
//        this.affineGapExtensionPenalty = affineGapExtensionPenalty;
//    }
//}
