package com.accakyra.lsss;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PersistenceTest extends TestBase {

    @Test
    void oneFolder() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
        } finally {
            Files.walk(data.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        assertFalse(data.exists());
        assertTrue(data.mkdir());
        try (DAO dao = createDao()) {
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void reopen() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
        }

        try (DAO dao = createDao()) {
            assertEquals(value, dao.get(key));
        }
    }

    @Test
    void delete() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
        }

        try (DAO dao = createDao()) {
            assertEquals(value, dao.get(key));
            dao.delete(key);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }

        try (DAO dao = createDao()) {
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @RepeatedTest(100)
    void replaceWithClose() throws Exception {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();
        final ByteBuffer value2 = randomValue();

        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
        }

        try (DAO dao = createDao()) {
            assertEquals(value, dao.get(key));
            dao.upsert(key, value2);
            assertEquals(value2, dao.get(key));
        }

        try (DAO dao = createDao()) {
            assertEquals(value2, dao.get(key));
        }
    }

    @Test
    void hugeKeys() throws IOException {
        final int size = 1024 * 1024;
        final ByteBuffer payload = randomBuffer(size);
        final ByteBuffer value = randomValue();
        final int records = 128;
        final Collection<ByteBuffer> keys = new ArrayList<>(records);

        try (DAO dao = createDao()) {
            for (int i = 0; i < records; i++) {
                final ByteBuffer key = randomKey();
                keys.add(key);
                final ByteBuffer hugeKey = join(key, payload);
                dao.upsert(hugeKey, value);
                assertEquals(value, dao.get(hugeKey));
            }
        }

        try (DAO dao = createDao()) {
            for (final ByteBuffer key : keys) {
                assertEquals(value, dao.get(join(key, payload)));
            }
        }
    }

    @Test
    void hugeValues() throws IOException {
        final int size = 1024 * 1024;
        final ByteBuffer suffix = randomBuffer(size);
        final int records = 128;
        final Collection<ByteBuffer> keys = new ArrayList<>(records);

        try (DAO dao = createDao()) {
            for (int i = 0; i < records; i++) {
                final ByteBuffer key = randomKey();
                final ByteBuffer value = join(key, suffix);
                keys.add(key);
                dao.upsert(key, value);
                assertEquals(value, dao.get(key));
            }
        }

        try (DAO dao = createDao()) {
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, suffix), dao.get(key));
            }
        }
    }

    @RepeatedTest(5)
    void manyRecords() throws IOException {
        final int recordsCount = 10_000;
        final Map<ByteBuffer, ByteBuffer> records = new HashMap<>(recordsCount);

        try (final DAO dao = createDao()) {
            for (int i = 0; i < recordsCount; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);

                records.put(key, value);
                assertEquals(value, dao.get(key));
            }
            for (Map.Entry<ByteBuffer, ByteBuffer> record : records.entrySet()) {
                assertEquals(record.getValue(), dao.get(record.getKey()));
            }
        }
    }

    @Test
    void replaceManyTimes() throws IOException {
        final ByteBuffer key = randomKey();
        final int overwrites = 100;

        for (int i = 0; i < overwrites; i++) {
            final ByteBuffer value = randomValue();
            try (DAO dao = createDao()) {
                dao.upsert(key, value);
                assertEquals(value, dao.get(key));
            }

            try (DAO dao = createDao()) {
                assertEquals(value, dao.get(key));
            }
        }
    }
}
