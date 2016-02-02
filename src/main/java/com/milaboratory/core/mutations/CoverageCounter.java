package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class CoverageCounter {
    final int refFrom, refTo;
    final long[] counters;

    CoverageCounter(int refFrom, int refTo, long[] counters) {
        this.refFrom = refFrom;
        this.refTo = refTo;
        this.counters = counters;
    }

    public CoverageCounter(Range seqRange) {
        this(seqRange.getFrom(), seqRange.getTo());
    }

    public CoverageCounter(int refFrom, int refTo) {
        this.refFrom = refFrom;
        this.refTo = refTo;
        this.counters = new long[refTo - refFrom];
    }

    public void aggregate(final Range r, final Weight weight) {
        final int from = r.getFrom(), to = r.getTo();
        if (from < refFrom || to > refTo)
            throw new IndexOutOfBoundsException();
        for (int i = from; i < to; ++i)
            counters[i] += weight.weight(i);
    }

    public void aggregate(final Range r) {
        aggregate(r, Weight.ONE);
    }

    public long totalWeight(int position) {
        return counters[position - refFrom];
    }
}
