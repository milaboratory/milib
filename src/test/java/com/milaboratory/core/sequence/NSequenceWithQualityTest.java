package com.milaboratory.core.sequence;

import com.milaboratory.core.io.util.IOTestUtil;
import org.junit.Test;

/**
 * Created by poslavsky on 15/04/15.
 */
public class NSequenceWithQualityTest {
    @Test
    public void test1() throws Exception {
        Object se = new NSequenceWithQuality(new NucleotideSequence("AACCTTGACC"), new SequenceQuality("++++++++++"));
        IOTestUtil.assertJavaSerialization(se);
    }
}