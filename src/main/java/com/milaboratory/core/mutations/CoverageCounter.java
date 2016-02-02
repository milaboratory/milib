package com.milaboratory.core.mutations;

import com.milaboratory.core.Range;

/**
 * @author Dmitry Bolotin
 * @author Stanislav Poslavsky
 */
public final class CoverageCounter {
    final int from, to;
    final long[] counters;

    CoverageCounter(int from, int to, long[] counters) {
        this.from = from;
        this.to = to;
        this.counters = counters;
    }

    public CoverageCounter(Range seqRange) {
        this(seqRange.getFrom(), seqRange.getTo());
    }

    public CoverageCounter(int from, int to) {
        this.from = from;
        this.to = to;
        this.counters = new long[to - from];
    }

    public void aggregate(final Range r, final Weight weight) {
        final int from = r.getFrom(), to = r.getTo();
        if (from < this.from || to > this.to)
            throw new IndexOutOfBoundsException();
        for (int i = from; i < to; ++i)
            counters[i] += weight.weight(i);
    }

    public void aggregate(final Range r) {
        aggregate(r, Weight.ONE);
    }

    public long totalWeight(int position) {
        if (position < from || position >= to)
            return 0;
        return counters[position - from];
    }
}
