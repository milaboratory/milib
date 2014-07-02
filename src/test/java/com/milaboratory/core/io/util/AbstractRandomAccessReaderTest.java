package com.milaboratory.core.io.util;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well1024a;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static com.milaboratory.core.io.util.TestUtil.*;

public class AbstractRandomAccessReaderTest {
    @Test
    public void test1() throws Exception {
        File tempFile = createRandomFile(System.currentTimeMillis());
        String[] allLines = getAllLines(tempFile);
        FileIndex index = buildIndex(tempFile, 5, 0);
        StringReader reader = new StringReader(index, tempFile);
        for (int i = 0; i < allLines.length; ++i)
            Assert.assertEquals(allLines[i], reader.take(i));
        tempFile.delete();
    }

    @Test
    public void test2() throws Exception {
        File tempFile = createRandomFile(System.currentTimeMillis());
        String[] allLines = getAllLines(tempFile);
        for (int step = 5; step < 10; step += 2) {
            for (int start = 1; start <= step; ++start) {
                FileIndex index = buildIndex(tempFile, step, start);
                StringReader reader = new StringReader(index, tempFile);
                for (int i = 0; i < allLines.length; ++i)
                    Assert.assertEquals(allLines[i], reader.take(i));
            }
        }
        tempFile.delete();
    }

    @Test
    public void test3() throws Exception {
        RandomGenerator rnd = new Well1024a();
        File tempFile = createRandomFile(rnd.nextLong());
        String[] allLines = getAllLines(tempFile);
        for (int step = 5; step < 10; step += 2) {
            for (int start = 1; start <= step; ++start) {
                FileIndex index = buildIndex(tempFile, step, start);
                StringReader reader = new StringReader(index, tempFile);
                for (int i = 0; i < allLines.length; ++i) {
                    int p = rnd.nextInt(allLines.length);
                    Assert.assertEquals(allLines[p], reader.take(p));
                }
            }
        }
        tempFile.delete();
    }
}