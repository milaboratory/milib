package com.milaboratory.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TempFileManager {
    private static volatile String prefix = "milib_";
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    static final ConcurrentHashMap<String, File> createdFiles = new ConcurrentHashMap<>();

    public static void setPrefix(String prefix) {
        TempFileManager.prefix = prefix;
    }

    public static File getTempFile() {
        try {
            if (initialized.compareAndSet(false, true))
                // Adding delete files shutdown hook on the very firs execution of getTempFile()
                Runtime.getRuntime().addShutdownHook(new Thread(new RemoveAction(), "DeleteTempFiles"));

            File file;
            String name;

            do {
                name = prefix + RandomUtil.getThreadLocalRandomData().nextHexString(40);
                file = Files.createTempFile(name, null).toFile();
            } while (createdFiles.putIfAbsent(name, file) != null);

            if (file.length() != 0)
                throw new RuntimeException();

            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File getTempDir() {
        try {
            if (initialized.compareAndSet(false, true))
                // Adding delete files shutdown hook on the very firs execution of getTempFile()
                Runtime.getRuntime().addShutdownHook(new Thread(new RemoveAction(), "DeleteTempFiles"));

            File dir;
            String name;

            do {
                name = prefix + RandomUtil.getThreadLocalRandomData().nextHexString(40);
                dir = Files.createTempDirectory(name).toFile();
            } while (createdFiles.putIfAbsent(name, dir) != null);

            return dir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class RemoveAction implements Runnable {
        @Override
        public void run() {
            for (File file : createdFiles.values()) {
                if (file.exists()) {
                    try {
                        if (Files.isDirectory(file.toPath()))
                            FileUtils.deleteDirectory(file);
                        else
                            file.delete();
                    } catch (Exception e) {
                    }
                }
            }
        }
    }
}
