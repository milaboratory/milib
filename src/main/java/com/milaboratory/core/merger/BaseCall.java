package com.milaboratory.core.merger;

public class BaseCall {
    private final byte base;
    private final double errorProbability;

    public BaseCall(byte base, byte quality) {
        this.base = base;
        this.errorProbability = Math.pow(10.0, -quality / 10.0);
    }

    public BaseCall(byte base, double errorProbability) {
        this.base = base;
        this.errorProbability = errorProbability;
    }

    public byte getBase() {
        return base;
    }

    public byte getQuality() {
        return (byte) (-10.0 * Math.log10(errorProbability));
    }

    public double getErrorProbability() {
        return errorProbability;
    }
}
