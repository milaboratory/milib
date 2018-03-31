/*
 * Copyright 2018 MiLaboratory.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.milaboratory.core.sequence.quality;

import com.milaboratory.core.sequence.SequenceQuality;
import com.milaboratory.core.sequence.SequenceQualityBuilder;
import org.junit.Assert;
import org.junit.Test;

public class QualityTrimmerTest {
    @Test
    public void test0() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(6, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, true, 7.0f));
        Assert.assertEquals(21, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, true, 7.0f));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, false, 7.0f));
        Assert.assertEquals(q0.size(), QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, false, 7.0f));
    }

    @Test
    public void test0Negative() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0000000000000000000000000000");
        Assert.assertEquals(-2 - (q0.size() - 1), QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, true, 7.0f));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, true, 7.0f));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, false, 7.0f));
        Assert.assertEquals(q0.size(), QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, false, 7.0f));
    }

    @Test
    public void test1() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(13, QualityTrimmer.trim(q0, 14, q0.size(), +1, 6, true, 7.0f));
        Assert.assertEquals(21, QualityTrimmer.trim(q0, 14, q0.size(), -1, 6, true, 7.0f));
        Assert.assertEquals(20, QualityTrimmer.trim(q0, 14, q0.size(), +1, 6, false, 7.0f));
        Assert.assertEquals(q0.size(), QualityTrimmer.trim(q0, 14, q0.size(), -1, 6, false, 7.0f));
    }

    @Test
    public void testBoundaryCase1() {
        SequenceQuality q0 = q("9");
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, true, 7.0f));
        Assert.assertEquals(1, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, true, 7.0f));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, false, 7.0f));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, false, 7.0f));
    }

    @Test
    public void testBoundaryCase2() {
        SequenceQuality q0 = q("0");
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, true, 7.0f));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, true, 7.0f));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, false, 7.0f));
        Assert.assertEquals(1, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, false, 7.0f));
    }

    @Test
    public void testBoundaryCase3() {
        SequenceQuality q0 = q("");
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, true, 7.0f));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, true, 7.0f));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, 6, false, 7.0f));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), -1, 6, false, 7.0f));
    }

    SequenceQuality q(String nuQuality) {
        SequenceQualityBuilder builder =
                new SequenceQualityBuilder()
                        .ensureCapacity(nuQuality.length());
        for (int i = 0; i < nuQuality.length(); i++)
            builder.append((byte) Character.digit(nuQuality.charAt(i), 16));
        return builder.createAndDestroy();
    }
}