package com.milaboratory.core.merger;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.milaboratory.core.sequence.NSequenceWithQuality;

/**
 * Created by poslavsky on 21/02/2017.
 */
public abstract class SequenceEqualityCheckStrategy {

    public abstract SequenceEqualityChecker make();

    public final boolean checkEquality(NSequenceWithQuality a, NSequenceWithQuality b,
                                       int aFrom, int bFrom, int length) {
        SequenceEqualityChecker checker = make();
        for (int i = 0; i < length; ++i)
            checker.put(
                    a.getSequence().codeAt(aFrom + i),
                    b.getSequence().codeAt(bFrom + i),
                    a.getQuality().value(aFrom + i),
                    b.getQuality().value(bFrom + i)
            );
        return checker.areSame();
    }

//    public final boolean checkEquality(NSequenceWithQuality a, NSequenceWithQuality b,
//                                       int overlap) {
//
//    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE)
    public final class UniformSequenceEqualityCheckStrategy extends SequenceEqualityCheckStrategy {
        private final int totalQualityThreshold;
        private final byte pcrQuality;

        @JsonCreator
        public UniformSequenceEqualityCheckStrategy(
                @JsonProperty("totalQualityThreshold") int totalQualityThreshold,
                @JsonProperty("pcrQuality") byte pcrQuality) {
            this.totalQualityThreshold = totalQualityThreshold;
            this.pcrQuality = pcrQuality;
        }

        @Override
        public SequenceEqualityChecker make() {
            return new UniformSequenceEqualityChecker(totalQualityThreshold, pcrQuality);
        }
    }
}
