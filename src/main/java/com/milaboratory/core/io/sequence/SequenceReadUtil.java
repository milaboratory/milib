package com.milaboratory.core.io.sequence;

public final class SequenceReadUtil {
    private SequenceReadUtil() {}

    public static SequenceRead setReadId(long readId, SequenceRead read) {
        int nReads = read.numberOfReads();
        SingleRead[] reads = new SingleRead[nReads];
        for (int i = 0; i < reads.length; i++)
            reads[i] = new SingleReadImpl(readId, read.getRead(i).getData(), read.getRead(i).getDescription());
        if (nReads == 1)
            return reads[0];
        else if (nReads == 2)
            return new PairedRead(reads);
        else
            return new MultiRead(reads);
    }
}
