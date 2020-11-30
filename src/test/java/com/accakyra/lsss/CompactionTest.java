package com.accakyra.lsss;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompactionTest extends TestBase {

    @Test
    void overwrite() throws IOException {
        int valueSize = 1024 * 1024;
        int keyCount = 100;
        int overwrites = 10;

        List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        for (int round = 0; round < overwrites; round++) {
            try (DAO dao = createDao()) {
                for (ByteBuffer key : keys) {
                    dao.upsert(key, randomBuffer(valueSize));
                }
            }
        }

        long size = directorySize();
        long minSize = keyCount * (KEY_LENGTH + valueSize);

        assertTrue(size < 2 * minSize);
    }

    @Test
    void multiple() throws IOException {
        int valueSize = 1024 * 1024;
        int keyCount = 100;
        int overwrites = 10;

        List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (DAO dao = createDao()) {
            for (int round = 0; round < overwrites; round++) {
                for (ByteBuffer key : keys) {
                    dao.upsert(key, randomBuffer(valueSize));
                }
            }
        }

        long size = directorySize();
        long minSize = keyCount * (KEY_LENGTH + valueSize);

        assertTrue(size < 3 * minSize);
    }

    @Test
    void clear() throws IOException {
        int valueSize = 1024 * 1024;
        int keyCount = 100;

        List<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                dao.upsert(key, randomBuffer(valueSize));
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                dao.delete(key);
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                dao.upsert(key, randomBuffer(valueSize));
            }
        }

        long size = directorySize();
        long maxSize = 2 * keyCount * (KEY_LENGTH + valueSize);

        assertTrue(size < maxSize);
    }
}
