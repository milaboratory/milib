package com.milaboratory.core.merger;

/**
 * Created by poslavsky on 21/02/2017.
 */
public final class UniformSequenceEqualityChecker implements SequenceEqualityChecker {
    private int sumQuality;
    private final int totalQualityThreshold;
    private final byte pcrQuality;

    public UniformSequenceEqualityChecker(int totalQualityThreshold, byte pcrQuality) {
        this.totalQualityThreshold = totalQualityThreshold;
        this.pcrQuality = pcrQuality;
    }

    @Override
    public UniformSequenceEqualityChecker put(byte a, byte b, byte aQuality, byte bQuality) {
        if (a != b)
            sumQuality += Math.min(aQuality, pcrQuality) + Math.min(bQuality, pcrQuality);
        return this;
    }

    @Override
    public boolean areSame() {
        return sumQuality <= totalQualityThreshold;
    }
}
