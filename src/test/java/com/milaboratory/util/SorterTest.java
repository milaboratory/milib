package com.milaboratory.util;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.OutputPortCloseable;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

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

    private static void testWithInts(int nElements, int chunkSize) throws Exception {
        File tmpFile = TempFileManager.getTempFile();

        ArrayList<Integer> source = new ArrayList<>();
        for (int i = 0; i < nElements; i++) {
            int e = RandomUtil.getThreadLocalRandom().nextInt();
            source.add(e);
        }

        Sorter.ObjectSerializer<Integer> intSerializer = new Sorter.ObjectSerializer<Integer>() {
            @Override
            public void write(Collection<Integer> data, OutputStream stream) {
                DataOutputStream out = new DataOutputStream(stream);
                try {
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

        int count = 0;
        Integer prev = null;
        for (Integer integer : CUtils.it(sorted)) {
            if (prev == null)
                prev = integer;
            else
                Assert.assertTrue(integer.compareTo(prev) >= 0);
            ++count;
        }

        Assert.assertEquals(nElements, count);
    }
}