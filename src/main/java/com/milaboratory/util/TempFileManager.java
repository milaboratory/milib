package com.milaboratory.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TempFileManager {
    private static volatile String prefix = "milib_";
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    static final ConcurrentHashMap<String, File> createdFiles = new ConcurrentHashMap<>();
    private static final RandomDataGenerator privateRandom = new RandomDataGenerator(new Well44497b());

    public static void setPrefix(String prefix) {
        TempFileManager.prefix = prefix;
    }

    public static void seed(long seed) {
        synchronized (privateRandom) {
            privateRandom.getRandomGenerator().setSeed(seed);
        }
    }

    private static void ensureInitialized() {
        if (initialized.compareAndSet(false, true)) {
            // Adding delete files shutdown hook on the very firs execution of getTempFile()
            Runtime.getRuntime().addShutdownHook(new Thread(new RemoveAction(), "DeleteTempFiles"));
            seed(System.nanoTime() + 17 * (new SecureRandom()).nextLong());
        }
    }

    public static File getTempFile() {
        return getTempFile(null, null);
    }

    public static File getTempFile(String suffix) {
        return getTempFile(null, suffix);
    }

    public static File getTempFile(Path tmpDir) {
        return getTempFile(tmpDir, null);
    }

    public static File getTempFile(Path tmpDir, String suffix) {
        try {
            ensureInitialized();

            File file;
            String name;

            do {
                synchronized (privateRandom) {
                    name = prefix + privateRandom.nextHexString(40);
                }
                file = tmpDir == null ? Files.createTempFile(name, suffix).toFile() : Files.createTempFile(tmpDir, name, suffix).toFile();
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
            ensureInitialized();

            File dir;
            String name;

            do {
                synchronized (privateRandom) {
                    name = prefix + privateRandom.nextHexString(40);
                }
                dir = Files.createTempDirectory(name).toFile();
            } while (createdFiles.putIfAbsent(name, dir) != null);

            return dir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Register temp directory or file. The file or directory will be deleted on JVM shutdown.
     * Anyway it is a good idea to delete the folder as early as possible.
     *
     * @param path file or directory
     */
    public static void register(File path) {
        createdFiles.put(path.getAbsolutePath(), path);
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
