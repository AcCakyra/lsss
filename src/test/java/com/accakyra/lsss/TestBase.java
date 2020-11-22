package com.accakyra.lsss;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

abstract class TestBase {

    static final int KEY_LENGTH = 16;
    static final int VALUE_LENGTH = 1024;

    @TempDir
    static File data;

    static ByteBuffer randomKey() {
        return randomBuffer(KEY_LENGTH);
    }

    static ByteBuffer randomValue() {
        return randomBuffer(VALUE_LENGTH);
    }

    static ByteBuffer randomBuffer(final int length) {
        final byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return ByteBuffer.wrap(result);
    }

    static ByteBuffer join(final ByteBuffer left, final ByteBuffer right) {
        final ByteBuffer result = ByteBuffer.allocate(left.remaining() + right.remaining());
        result.put(left.duplicate());
        result.put(right.duplicate());
        result.rewind();
        return result;
    }

    static DAO createDao() {
        return DAOFactory.create(data);
    }

    @AfterEach
    void cleanStorage() {
        Arrays.stream(data.listFiles())
                .sorted(Comparator.reverseOrder())
                .forEach(File::delete);
    }

    ByteBuffer stringToByteBuffer(String data) {
        return ByteBuffer.wrap(data.getBytes());
    }

    static long directorySize() throws IOException {
        final AtomicLong result = new AtomicLong(0L);
        java.nio.file.Files.walkFileTree(
                data.toPath(),
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(
                            Path file,
                            BasicFileAttributes attrs) {
                        result.addAndGet(attrs.size());
                        return FileVisitResult.CONTINUE;
                    }
                });
        return result.get();
    }
}
