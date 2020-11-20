package com.accakyra.lsss;

import com.accakyra.lsss.lsm.Config;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceTest extends TestBase {

    @Test
    void cleanDirectory() throws IOException {
        ByteBuffer key = randomKey();
        ByteBuffer value = randomValue();

        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
        } finally {
            cleanStorage();
        }

        try (DAO dao = createDao()) {
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void reopen() throws IOException {
        ByteBuffer key = randomKey();
        ByteBuffer value = randomValue();

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
        ByteBuffer key = randomKey();
        ByteBuffer value = randomValue();

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


    @Test
    void deleteMany() throws IOException {
        int tombstonesCount = 100000;

        try (DAO dao = createDao()) {
            Iterator<ByteBuffer> tombstones =
                    Stream.generate(TestBase::randomKey)
                            .limit(tombstonesCount)
                            .iterator();
            while (tombstones.hasNext()) {
                dao.delete(tombstones.next());
            }

            Iterator<Record> empty = dao.iterator();
            assertFalse(empty.hasNext());
        }
    }

    @RepeatedTest(100)
    void replaceWithClose() throws Exception {
        ByteBuffer key = randomKey();
        ByteBuffer value = randomValue();
        ByteBuffer value2 = randomValue();

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
        int size = Config.MEMTABLE_THRESHOLD;
        ByteBuffer payload = randomBuffer(size);
        ByteBuffer value = randomValue();
        int records = 128;
        Collection<ByteBuffer> keys = new ArrayList<>(records);

        try (DAO dao = createDao()) {
            for (int i = 0; i < records; i++) {
                ByteBuffer key = randomKey();
                keys.add(key);
                ByteBuffer hugeKey = join(key, payload);
                dao.upsert(hugeKey, value);
                assertEquals(value, dao.get(hugeKey));
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                assertEquals(value, dao.get(join(key, payload)));
            }
        }
    }

    @Test
    void hugeValues() throws IOException {
        int size = Config.MEMTABLE_THRESHOLD;
        ByteBuffer suffix = randomBuffer(size);
        int records = 128;
        Collection<ByteBuffer> keys = new ArrayList<>(records);

        try (DAO dao = createDao()) {
            for (int i = 0; i < records; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = join(key, suffix);
                keys.add(key);
                dao.upsert(key, value);
                assertEquals(value, dao.get(key));
            }
        }

        try (DAO dao = createDao()) {
            for (ByteBuffer key : keys) {
                assertEquals(join(key, suffix), dao.get(key));
            }
        }
    }

    @RepeatedTest(100)
    void manyRecords() throws IOException {
        int recordsCount = 10_000;
        Map<ByteBuffer, ByteBuffer> records = new HashMap<>(recordsCount);

        try (DAO dao = createDao()) {
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

    @RepeatedTest(10)
    void manyRecordsAfterClose() throws IOException {
        int recordsCount = 10_000;
        Map<ByteBuffer, ByteBuffer> records = new HashMap<>(recordsCount);

        try (DAO dao = createDao()) {
            for (int i = 0; i < recordsCount; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                records.put(key, value);

                assertEquals(value, dao.get(key));
            }
        }
        try (DAO dao = createDao()) {
            for (Map.Entry<ByteBuffer, ByteBuffer> record : records.entrySet()) {
                assertEquals(record.getValue(), dao.get(record.getKey()));
            }
        }
    }

    @RepeatedTest(10)
    void overwrite() throws IOException {
        int valueSize = 1024 * 1024;
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
    }

    @Test
    void clear() throws IOException {
        int valueSize = Config.MEMTABLE_THRESHOLD;
        int keyCount = 10;

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
    }

    @Test
    void iteration() throws IOException {
        int count = 1000;
        NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        try (DAO dao = createDao()) {
            for (int i = 0; i < count; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                map.put(key, value);
            }
        }

        int fromIndex = new Random().nextInt(count / 2);
        int toIndex = fromIndex + new Random().nextInt(count / 2);
        ByteBuffer from = map.entrySet().stream().skip(fromIndex).findFirst().get().getKey();
        ByteBuffer to = map.entrySet().stream().skip(toIndex).findFirst().get().getKey();

        try (DAO dao = createDao()) {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> expectedIter = map.subMap(from, to).entrySet().iterator();
            Iterator<Record> actualIter = dao.iterator(from, to);

            while (expectedIter.hasNext()) {
                Map.Entry<ByteBuffer, ByteBuffer> expected = expectedIter.next();
                Record actual = actualIter.next();
                assertEquals(expected.getKey(), actual.getKey());
                assertEquals(expected.getValue(), actual.getValue());

                dao.upsert(randomKey(), randomValue());
            }
            assertFalse(actualIter.hasNext());
        }
    }

    @Test
    void iterationWithFlushing() throws IOException {
        NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        try (DAO dao = createDao()) {
            int count = 1000;
            for (int i = 0; i < count; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                map.put(key, value);
            }

            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> expectedIter = map.entrySet().iterator();
            Iterator<Record> actualIter = dao.iterator();

            int insertionCountToFlushMemtable =
                    Config.MEMTABLE_THRESHOLD / (TestBase.VALUE_LENGTH + TestBase.KEY_LENGTH) + 1;

            for (int i = 0; i < insertionCountToFlushMemtable; i++) {
                dao.upsert(randomKey(), randomValue());
            }

            while (expectedIter.hasNext()) {
                Map.Entry<ByteBuffer, ByteBuffer> expected = expectedIter.next();
                Record actual = actualIter.next();
                assertEquals(expected.getKey(), actual.getKey());
                assertEquals(expected.getValue(), actual.getValue());
            }
            assertFalse(actualIter.hasNext());
        }
    }

    @Test
    void dao() throws IOException {
        try (DAO dao = createDao()) {
            dao.upsert(stringToByteBuffer("AAA1"), randomValue());
            dao.upsert(stringToByteBuffer("AAA2"), randomValue());
            dao.upsert(stringToByteBuffer("AAA3"), randomValue());
            dao.upsert(stringToByteBuffer("AAA4"), randomValue());
            dao.upsert(stringToByteBuffer("BBB1"), randomValue());
            dao.upsert(stringToByteBuffer("BBB2"), randomValue());
            dao.upsert(stringToByteBuffer("CCC1"), randomValue());
            dao.upsert(stringToByteBuffer("ZZZ1"), randomValue());
            dao.upsert(stringToByteBuffer("ZZZ2"), randomValue());
        }

        try (DAO dao = createDao()) {
            assertEquals(9, calcIteratorSize(dao.iterator()));

            assertEquals(4, calcIteratorSize(dao.iterator(stringToByteBuffer("BBB2"))));
            assertEquals(5, calcIteratorSize(dao.iterator(stringToByteBuffer("AAA5"))));

            assertEquals(9, calcIteratorSize(dao.iterator(
                    stringToByteBuffer("A"),
                    stringToByteBuffer("ZZZZZ")))
            );
            assertEquals(1, calcIteratorSize(dao.iterator(
                    stringToByteBuffer("AAA3"),
                    stringToByteBuffer("AAA4")))
            );
            assertEquals(1, calcIteratorSize(dao.iterator(
                    stringToByteBuffer("BBB3"),
                    stringToByteBuffer("CCC2")))
            );
        }
    }
}
