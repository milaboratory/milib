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

import com.milaboratory.core.Range;
import com.milaboratory.core.sequence.SequenceQuality;
import com.milaboratory.core.sequence.SequenceQualityBuilder;
import com.milaboratory.test.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public class QualityTrimmerTest {
    final QualityTrimmerParameters params7 = new QualityTrimmerParameters(7.0f, 6);

    @Test
    public void test0() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(6, QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(21, QualityTrimmer.trim(q0, 0, q0.size(), -1, true, params7));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, false, params7));
        Assert.assertEquals(q0.size(), QualityTrimmer.trim(q0, 0, q0.size(), -1, false, params7));
    }

    @Test
    public void test0Negative() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0000000000000000000000000000");
        Assert.assertEquals(-2 - (q0.size() - 1), QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), -1, true, params7));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, false, params7));
        Assert.assertEquals(q0.size(), QualityTrimmer.trim(q0, 0, q0.size(), -1, false, params7));
    }

    @Test
    public void test1() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(13, QualityTrimmer.trim(q0, 14, q0.size(), +1, true, params7));
        Assert.assertEquals(21, QualityTrimmer.trim(q0, 14, q0.size(), -1, true, params7));
        Assert.assertEquals(20, QualityTrimmer.trim(q0, 14, q0.size(), +1, false, params7));
        Assert.assertEquals(q0.size(), QualityTrimmer.trim(q0, 14, q0.size(), -1, false, params7));
    }

    @Test
    public void test2() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(1, QualityTrimmer.trim(q0, 2, q0.size(), +1, false, params7));
    }

    @Test
    public void test3() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("499999999941");
        Assert.assertEquals(0, QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(9, QualityTrimmer.pabs(QualityTrimmer.trim(q0, 4, q0.size(), +1, false, params7)));
    }

    @Test
    public void test4() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("4999999999429");
        Assert.assertEquals(0, QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(12, QualityTrimmer.pabs(QualityTrimmer.trim(q0, 4, q0.size(), +1, false, params7)));
    }

    @Test
    public void testBoundaryCase1() {
        SequenceQuality q0 = q("9");
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(1, QualityTrimmer.trim(q0, 0, q0.size(), -1, true, params7));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), +1, false, params7));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), -1, false, params7));
    }

    @Test
    public void testBoundaryCase2() {
        SequenceQuality q0 = q("0");
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(-2, QualityTrimmer.trim(q0, 0, q0.size(), -1, true, params7));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, false, params7));
        Assert.assertEquals(1, QualityTrimmer.trim(q0, 0, q0.size(), -1, false, params7));
    }

    @Test
    public void testBoundaryCase3() {
        SequenceQuality q0 = q("");
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, true, params7));
        Assert.assertEquals(0, QualityTrimmer.trim(q0, 0, q0.size(), -1, true, params7));
        Assert.assertEquals(-1, QualityTrimmer.trim(q0, 0, q0.size(), +1, false, params7));
        Assert.assertEquals(0, QualityTrimmer.trim(q0, 0, q0.size(), -1, false, params7));
    }

    @Test
    public void extendTest1() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(new Range(7, 21), QualityTrimmer.extendRange(q0, params7, new Range(10, 15)));
        Assert.assertEquals(new Range(7, 21), QualityTrimmer.extendRange(q0, params7, new Range(7, 21)));
        Assert.assertEquals(new Range(6, 21), QualityTrimmer.extendRange(q0, params7, new Range(6, 21)));
        Assert.assertEquals(new Range(5, 21), QualityTrimmer.extendRange(q0, params7, new Range(5, 21)));
        Assert.assertEquals(new Range(5, 22), QualityTrimmer.extendRange(q0, params7, new Range(5, 22)));
        Assert.assertEquals(new Range(5, 23), QualityTrimmer.extendRange(q0, params7, new Range(5, 23)));
    }

    @Test
    public void extendTest2() {
        SequenceQuality q0 = q("");
        Assert.assertEquals(new Range(0, 0), QualityTrimmer.extendRange(q0, params7, new Range(0, 0)));
    }

    @Test
    public void extendTest3() {
        SequenceQuality q0 = q("1");
        Assert.assertEquals(new Range(0, 0), QualityTrimmer.extendRange(q0, params7, new Range(0, 0)));
    }

    @Test
    public void extendTest4() {
        SequenceQuality q0 = q("9");
        Assert.assertEquals(new Range(0, 1), QualityTrimmer.extendRange(q0, params7, new Range(0, 0)));
    }

    @Test
    public void trimTest0() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertEquals(new Range(7, 21), QualityTrimmer.trim(q0, params7));
        Assert.assertNull(QualityTrimmer.trim(q0, params7, new Range(1, 5)));
        Assert.assertEquals(new Range(10, 13), QualityTrimmer.trim(q0, params7, new Range(10, 13)));
    }

    @Test
    public void trimTest2() {
        SequenceQuality q0 = q("");
        Assert.assertNull(QualityTrimmer.trim(q0, params7));
    }

    @Test
    public void trimTest3() {
        SequenceQuality q0 = q("1");
        Assert.assertNull(QualityTrimmer.trim(q0, params7));
    }

    @Test
    public void trimTest4() {
        SequenceQuality q0 = q("9");
        Assert.assertEquals(new Range(0, 1), QualityTrimmer.trim(q0, params7));
    }

    @Test
    public void islandsTest1() {
        //                               |0        |10       |20
        SequenceQuality q0 = q("0123456789999999999876543210");
        Assert.assertArrayEquals(new Range[]{new Range(7, 21)},
                QualityTrimmer.findIslands(q0, params7, 0, +1, false));
        Assert.assertArrayEquals(new Range[]{new Range(7, 21)},
                QualityTrimmer.findIslands(q0, params7, q0.size() - 1, -1, false));

        Assert.assertArrayEquals(new Range[]{new Range(10, 21)},
                QualityTrimmer.findIslands(q0, params7, 10, +1, false));
        Assert.assertArrayEquals(new Range[]{new Range(7, 18)},
                QualityTrimmer.findIslands(q0, params7, 18, -1, false));
    }

    @Test
    public void islandsTest2() {
        //                               |0        |10       |20       |30       |40       |50
        SequenceQuality q0 = q("01234567899999999998765432100123456789999999999876543210");
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 49)},
                QualityTrimmer.findIslands(q0, params7, 0, +1, false));
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 49)},
                QualityTrimmer.findIslands(q0, params7, q0.size() - 1, -1, false));

        Assert.assertArrayEquals(new Range[]{new Range(10, 21), new Range(35, 49)},
                QualityTrimmer.findIslands(q0, params7, 10, +1, false));
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 46)},
                QualityTrimmer.findIslands(q0, params7, 46, -1, false));
    }

    @Test
    public void islandsFromRegionTest1() {
        //                               |0        |10       |20       |30       |40       |50       |60
        SequenceQuality q0 = q("012345678999999999987654321001234567899999999998765432100120102020");
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 49)},
                QualityTrimmer.calculateIslandsFromInitialRange(q0, params7, new Range(10, 20)));
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 49)},
                QualityTrimmer.calculateIslandsFromInitialRange(q0, params7, new Range(38, 40)));

        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(30, 49)},
                QualityTrimmer.calculateIslandsFromInitialRange(q0, params7, new Range(30, 40)));
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 52)},
                QualityTrimmer.calculateIslandsFromInitialRange(q0, params7, new Range(38, 52)));

        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 49), new Range(55, 59)},
                QualityTrimmer.calculateIslandsFromInitialRange(q0, params7, new Range(55, 59)));
        Assert.assertArrayEquals(new Range[]{new Range(0, 2), new Range(7, 21), new Range(35, 49)},
                QualityTrimmer.calculateIslandsFromInitialRange(q0, params7, new Range(0, 2)));
    }

    @Test
    public void islandsFromRegionTest2() {
        //                               |0        |10       |20       |30       |40       |50       |60
        SequenceQuality q0 = q("012345678999999999987654321001234567899999999998765432100120102020");
        Assert.assertArrayEquals(new Range[]{new Range(7, 21), new Range(35, 49)},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0        |10       |20       |30       |40       |50       |60
        q0 = q("999999999999999999987654321001234567899999999998765432100120102020");
        Assert.assertArrayEquals(new Range[]{new Range(0, 21), new Range(35, 49)},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0        |10       |20       |30       |40       |50       |60
        q0 = q("9999999999999999999876543210012345678999999999999999999999999");
        Assert.assertArrayEquals(new Range[]{new Range(0, 21), new Range(35, 61)},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0        |10       |20       |30       |40       |50       |60
        q0 = q("9999999999999999999899999999999909999999999978999999999999999");
        Assert.assertArrayEquals(new Range[]{new Range(0, 61)},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0
        q0 = q("999999");
        Assert.assertArrayEquals(new Range[]{new Range(0, 6)},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0
        q0 = q("");
        Assert.assertArrayEquals(new Range[]{},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0        |10
        q0 = q("4321001234567");
        Assert.assertArrayEquals(new Range[]{},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0        |10
        q0 = q("4321001234FFFF77");
        Assert.assertArrayEquals(new Range[]{new Range(10, 16)},
                QualityTrimmer.calculateAllIslands(q0, params7));

        //               |0        |10
        q0 = q("49999999994");
        Assert.assertArrayEquals(new Range[]{new Range(1, 10)},
                QualityTrimmer.calculateAllIslands(q0, params7));
    }

    @Test
    public void testParametersSerialization0() {
        TestUtil.assertJson(params7, true);
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