package com.milaboratory.core.sequence;

import com.milaboratory.core.io.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by poslavsky on 15/04/15.
 */
public class NSequenceWithQualityTest {
    @Test
    public void test1() throws Exception {
        Object se = new NSequenceWithQuality(new NucleotideSequence("AACCTTGACC"), new SequenceQuality("++++++++++"));
        TestUtil.assertJavaSerialization(se);
    }
}