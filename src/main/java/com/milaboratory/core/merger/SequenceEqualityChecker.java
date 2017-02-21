package com.milaboratory.core.merger;

import com.milaboratory.core.sequence.NSequenceWithQuality;

/**
 * Created by poslavsky on 21/02/2017.
 */
public interface SequenceEqualityChecker {
    SequenceEqualityChecker put(byte a, byte b, byte aQuality, byte bQuality);

    boolean areSame();
}
