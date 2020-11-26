package com.accakyra.lsss;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class CompactionTest extends TestBase {

    @Test
    void overwrite() throws IOException {
        int valueSize = 2 * 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        ByteBuffer value = randomBuffer(valueSize);
        List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        for (int round = 0; round < overwrites; round++) {
            try (DAO dao = createDao()) {
                for (ByteBuffer key : keys) {
                    dao.upsert(key, join(key, value));
                }
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }
        }

        long size = directorySize();
        long minSize = keyCount * (KEY_LENGTH + KEY_LENGTH + valueSize);

        assertTrue(size < 2 * minSize);
    }

    @Test
    void multiple() throws IOException {
        int valueSize = 2 * 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (DAO dao = createDao()) {
            for (int round = 0; round < overwrites; round++) {
                ByteBuffer payload = randomBuffer(valueSize);

                for (ByteBuffer key : keys) {
                    ByteBuffer value = join(key, payload);
                    dao.upsert(key, value);
                }

                for (ByteBuffer key : keys) {
                    ByteBuffer value = join(key, payload);
                    assertEquals(value, dao.get(key));
                }
            }
        }

        long size = directorySize();
        long minSize = keyCount * (KEY_LENGTH + KEY_LENGTH + valueSize);

        assertTrue(size < 2 * minSize);
    }

    @Test
    void clear() throws IOException {
        int valueSize = 1024 * 1024;
        int keyCount = 40;

        ByteBuffer value = randomBuffer(valueSize);
        List<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                dao.upsert(key, join(key, value));
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            for (ByteBuffer key : keys) {
                dao.delete(key);
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                dao.upsert(key, join(key, value));
            }
        }

        long size = directorySize();

        assertTrue(size < 2 * keyCount * valueSize);
    }
}
