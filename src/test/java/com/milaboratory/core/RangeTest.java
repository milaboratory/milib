/*
 * Copyright 2015 MiLaboratory.com
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
package com.milaboratory.core;

import com.milaboratory.core.io.util.IOTestUtil;
import com.milaboratory.test.TestUtil;
import org.junit.Test;

import static org.junit.Assert.*;

public class RangeTest {
    private Range r(int from, int to) {
        return new Range(from, to);
    }

    @Test
    public void testValues() throws Exception {
        assertEquals(20, r(20, 10).getUpper());
        assertEquals(10, r(20, 10).getLower());
        assertEquals(20, r(20, 10).getFrom());
        assertEquals(10, r(20, 10).getTo());

        assertEquals(20, r(10, 20).getUpper());
        assertEquals(10, r(10, 20).getLower());
        assertEquals(10, r(10, 20).getFrom());
        assertEquals(20, r(10, 20).getTo());
    }

    @Test
    public void testContains() throws Exception {
        assertFalse(r(20, 10).contains(20));
        assertTrue(r(20, 10).contains(19));

        assertFalse(r(20, 10).contains(r(21, 10)));
        assertTrue(r(20, 10).contains(r(20, 10)));
        assertTrue(r(20, 10).contains(r(19, 11)));
        assertFalse(r(20, 10).contains(r(21, 9)));
    }

    @Test
    public void testIntersection() throws Exception {
        assertEquals(r(10, 20).intersection(r(14, 40)), r(14, 20));
        assertEquals(r(10, 20).intersection(r(21, 40)), null);
    }

    @Test
    public void testMerge() throws Exception {
        assertEquals(r(10, 20).tryMerge(r(14, 40)), r(10, 40));
        assertEquals(r(10, 20).tryMerge(r(21, 40)), null);
    }

    @Test
    public void testIntersects() throws Exception {
        assertTrue(r(10, 20).intersectsWith(r(20, 10)));
        assertTrue(r(10, 20).intersectsWith(r(19, 11)));
        assertTrue(r(19, 11).intersectsWith(r(20, 10)));
    }

    @Test
    public void testCheckConvert1() throws Exception {
        checkConvert(r(10, 20), 10);
        checkConvert(r(10, 20), 13);
        checkConvert(r(20, 10), 19);
        checkConvert(r(20, 10), 18);
    }

    @Test
    public void test23e14() throws Exception {
        System.out.println(Integer.toBinaryString(((byte) 'A')));
        System.out.println(Integer.toBinaryString(((byte) 'T')));
        System.out.println(Integer.toBinaryString(((byte) 'G')));
        System.out.println(Integer.toBinaryString(((byte) 'C')));
        System.out.println();
        System.out.println(Integer.toBinaryString(((byte) 'B')));
        //System.out.println(Integer.toBinaryString(~(((byte) 'A') | ((byte) 'T') | ((byte) 'G') | ((byte) 'C'))));
    }

    private void checkConvert(Range range, int position) {
        int relativePosition = range.convertPointToRelativePosition(position);
        assertEquals(position, range.convertPointToAbsolutePosition(relativePosition));

        relativePosition = range.convertBoundaryToRelativePosition(position);
        assertEquals(position, range.convertBoundaryToAbsolutePosition(relativePosition));
    }

    @Test
    public void testRelativeRange() throws Exception {
        assertEquals(r(20, 40), r(0, 100).getRelativeRangeOf(r(20, 40)));
        assertEquals(r(20, 40), r(50, 150).getRelativeRangeOf(r(70, 90)));
        assertEquals(r(80, 90), r(100, 0).getRelativeRangeOf(r(20, 10)));
        assertEquals(r(90, 80), r(100, 0).getRelativeRangeOf(r(10, 20)));
    }

    @Test
    public void test4() throws Exception {
        Range se = new Range(3, 5);
        IOTestUtil.assertJavaSerialization(se);
    }

    @Test
    public void test5() throws Exception {
        Range se = new Range(3, 5);
        TestUtil.assertJson(se);
    }
}
