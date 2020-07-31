/*
 * Copyright 2020 MiLaboratory, LLC
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
package com.milaboratory.util.sorting;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import com.milaboratory.util.ObjectSerializer;
import com.milaboratory.util.RandomUtil;
import com.milaboratory.util.TempFileManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * Created by poslavsky on 28/02/2017.
 */
public class SorterTest {
    @Test
    public void test1() throws Exception {
        for (int nElements : new int[]{100_132, 10_000}) {
            testWithInts(nElements, nElements / 100);
            testWithInts(nElements, nElements / 10);
            testWithInts(nElements, nElements * 1);
            testWithInts(nElements, nElements * 2);
            testWithInts(nElements, nElements * 10);
        }
    }

    @Test
    public void test2() throws Exception {
        testWithInts(0, 10);
    }

    private static void testWithInts(int nElements, int chunkSize) throws Exception {
        File tmpFile = TempFileManager.getTempFile();

        ArrayList<Integer> source = new ArrayList<>();
        for (int i = 0; i < nElements; i++) {
            int e = RandomUtil.getThreadLocalRandom().nextInt();
            source.add(e);
        }

        ObjectSerializer<Integer> intSerializer = new ObjectSerializer<Integer>() {
            @Override
            public void write(Collection<Integer> data, OutputStream stream) {
                try (DataOutputStream out = new DataOutputStream(stream)) {
                    for (Integer datum : data) {
                        out.writeInt(datum);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public OutputPort<Integer> read(InputStream stream) {
                final DataInputStream in = new DataInputStream(stream);
                return new OutputPort<Integer>() {
                    @Override
                    public Integer take() {
                        try {
                            return in.readInt();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            }
        };

        OutputPortCloseable<Integer> sorted = Sorter.sort(CUtils.asOutputPort(source), new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        }, chunkSize, intSerializer, tmpFile);


        List<Integer> result = new ArrayList<>();
        for (Integer integer : CUtils.it(sorted))
            result.add(integer);


        Collections.sort(source);
        Assert.assertEquals(source, result);
    }
}