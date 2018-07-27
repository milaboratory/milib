package com.milaboratory.core.merger;

//
// As described in
//
// Error filtering, pair assembly and error correction for next-generation sequencing reads
//        Robert C. Edgar and Henrik Flyvbjerg
//
//
// [https://watermark.silverchair.com/btv401.pdf]
//
// To be used iteratively

public class BaseCallQualityMerger {
    private final double Wfactor, Gfactor;

    public BaseCallQualityMerger(int alphabetSize) { // 4 for ATGC, 5 for ATGC + gap
        this.Wfactor = (alphabetSize - 2) / (double) (alphabetSize - 1);
        this.Gfactor = 1.0 - Wfactor;
    }

    public BaseCall merge(BaseCall c1, BaseCall c2) {
        double p1 = c1.getErrorProbability(),
                p2 = c2.getErrorProbability();

        if (c1.getBase() == c2.getBase()) {
            // agree
            double pG = p1 * p2,           // both wrong and agree   - p1 * p2 * (alphabet size - 2) / (alphabet size - 1)
                    pC = 1 - p1 - p2 - pG; // both correct and agree - (1 - p1) * (1 - p2)
            pG *= Gfactor;
            return new BaseCall(c1.getBase(), pG / (pC + pG));
        } else {
            double pW = p1 * p2,  // wrong and disagree          -  p1 * p2 / (alphabet size - 1)
                    pR = p1 - pW, // first wrong second correct  -  p1 * (1 - p2)
                    pF = p2 - pW; // first correct second wrong  -  p2 * (1 - p1)
            pW *= Wfactor;
            if (p1 < p2) {
                // take first
                return new BaseCall(c1.getBase(), (pR + pW) / (pR + pW + pF));
            } else {
                // take second
                return new BaseCall(c2.getBase(), (pF + pW) / (pR + pW + pF));
            }
        }
    }
}
