package com.milaboratory.util;

import cc.redberry.pipe.CUtils;
import cc.redberry.pipe.OutputPort;
import cc.redberry.pipe.util.CountLimitingOutputPort;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

        AtomicInteger hash = new AtomicInteger();

        OutputPort<TestObject> source = new OutputPort<TestObject>() {
            @Override
            public synchronized TestObject take() {
                int e = RandomUtil.getThreadLocalRandom().nextInt();
                byte[] aaa = new byte[256];
                RandomUtil.getThreadLocalRandom().nextBytes(aaa);
                TestObject testObject = new TestObject(RandomUtil.getThreadLocalRandom().nextLong(), Arrays.toString(aaa));
                hash.addAndGet(testObject.hashCode());
                return testObject;
            }
        };

        source = new CountLimitingOutputPort<>(source, nElements);

        Sorter2.Serializer<Long> serK = new Sorter2.Serializer<Long>() {
            @Override
            public void serialize(Long l, DataOutput dest) throws IOException {
                dest.writeUTF("" + l);
            }

            @Override
            public Long deserialize(DataInput source) throws IOException {
                return Long.valueOf(source.readUTF());
            }
        };

        Sorter2.Serializer<TestObject> serO = new Sorter2.Serializer<TestObject>() {
            @Override
            public void serialize(TestObject testObject, DataOutput dest) throws IOException {
                dest.writeLong(testObject.l);
                dest.writeUTF(testObject.string);
            }

            @Override
            public TestObject deserialize(DataInput source) throws IOException {
                long l = source.readLong();
                String string = source.readUTF();
                return new TestObject(l, string);
            }
        };

        Sorter2<Long, TestObject> sorter = new Sorter2<>(source,
                o -> o.l, Long::compareTo,
                () -> serO, () -> serK,
                Executors.newCachedThreadPool(),
                8,
                nOpenFileStreams, chunkSize, tmpFile.toPath());

        int count = 0, hash1 = 0;
        TestObject prev = null;
        for (TestObject o : CUtils.it(sorter.run())) {
            if (prev != null)
                Assert.assertTrue(prev.l <= o.l);
            prev = o;
            ++count;
            hash1 += o.hashCode();
        }

        Assert.assertEquals(count, nElements);
        Assert.assertEquals(hash.get(), hash1);
    }

    public static final class TestObject {
        long l;
        String string;

        public TestObject(long l, String string) {
            this.l = l;
            this.string = string;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestObject)) return false;
            TestObject that = (TestObject) o;
            return l == that.l &&
                    Objects.equals(string, that.string);
        }

        @Override
        public int hashCode() {
            return Objects.hash(l, string);
        }
    }
}