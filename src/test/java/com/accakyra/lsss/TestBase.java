package com.accakyra.lsss;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

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

    int calcIteratorSize(Iterator<?> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    ByteBuffer stringToByteBuffer(String data) {
        return ByteBuffer.wrap(data.getBytes());
    }
}
