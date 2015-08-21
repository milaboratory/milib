package com.milaboratory.util;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;

public class RemoveActionTest {
    @Test
    public void test1() throws Exception {
        File tempDir = TempFileManager.getTempDir();
        FileUtils.write(tempDir.toPath().resolve("figure").toFile(), "sadfasf");
        System.out.println(tempDir);
    }

    @Test
    public void test2() throws Exception {
        File tempFile = TempFileManager.getTempFile();
        FileUtils.write(tempFile, "sadfasf");
        System.out.println(tempFile);
    }
}