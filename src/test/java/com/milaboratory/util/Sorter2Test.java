package com.milaboratory.util;

import cc.redberry.pipe.CUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 *
 */
public class Sorter2Test {
    @Test
    public void test1() throws Exception {
//        testWithInts(10000, 1/ 1000 );
        for (int nElements : new int[]{1_000_132, 100_132, 10_000}) {
            testWithInts(nElements, nElements * 12 / 4, 12);
        }
    }

    @Test
    public void asdasd() {
        System.out.println("as");
    }

    @Test
    public void test2() throws Exception {
        testWithInts(0, 10, 10);
    }

    private static void testWithInts(int nElements, long chunkSize, int nOpenFileStreams) throws Exception {
        File tmpFile = TempFileManager.getTempFile();

        ArrayList<Integer> source = new ArrayList<>();
        for (int i = 0; i < nElements; i++) {
            int e = RandomUtil.getThreadLocalRandom().nextInt();
            source.add(e);
        }

        Sorter2.Serializer<Integer> ser = new Sorter2.Serializer<Integer>() {
            @Override
            public void serialize(Integer integer, DataOutput dest) throws IOException {
                dest.writeInt(integer);
            }

            @Override
            public Integer deserialize(DataInput source) throws IOException {
                return source.readInt();
            }
        };

        Sorter2<Integer, Integer> sorter = new Sorter2<>(CUtils.asOutputPort(source),
                Function.identity(), Integer::compareTo,
                () -> ser, () -> ser,
                Executors.newCachedThreadPool(),
                8,
                nOpenFileStreams, chunkSize, tmpFile);

        List<Integer> result = new ArrayList<>();
        for (Integer integer : CUtils.it(sorter.run()))
            result.add(integer);


        Collections.sort(source);
        Assert.assertEquals(source, result);
    }
}