package com.accakyra.lsss;

import com.accakyra.lsss.lsm.Record;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryTest extends TestBase {

    @Test
    public void empty() throws IOException {
        try (DAO dao = createDao()) {
            assertThrows(NoSuchElementException.class, () -> dao.get(randomKey()));
        }
    }

    @Test
    public void insert() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();
        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
        }
    }

    @Test
    public void upsert() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value1 = randomValue();
        final ByteBuffer value2 = randomValue();
        try (DAO dao = createDao()) {
            dao.upsert(key, value1);
            assertEquals(value1, dao.get(key));
            assertEquals(value1, dao.get(key.duplicate()));
            dao.upsert(key, value2);
            assertEquals(value2, dao.get(key));
            assertEquals(value2, dao.get(key.duplicate()));
        }
    }

    @Test
    public void delete() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();
        try (DAO dao = createDao()) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            dao.delete(key);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    public void deleteAbsent() throws IOException {
        final ByteBuffer key = randomKey();
        try (DAO dao = createDao()) {
            dao.delete(key);
        }
    }

    @Test
    void emptyIterator() throws IOException {
        try (DAO dao = createDao()) {
            ByteBuffer from = randomKey();
            ByteBuffer to = randomKey();
            assertEquals(0, calcIteratorSize(dao.iterator(from, to)));
        }
    }

    @Test
    void iterationSize() throws IOException {
        try (DAO dao = createDao()) {
            int count = 100;
            NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                map.put(key, value);
            }

            int fromIndex = new Random().nextInt(count / 2);
            int toIndex = fromIndex + new Random().nextInt(count / 2);
            ByteBuffer from = map.entrySet().stream().skip(fromIndex).findFirst().get().getKey();
            ByteBuffer to = map.entrySet().stream().skip(toIndex).findFirst().get().getKey();

            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> expectedIter = map.subMap(from, to).entrySet().iterator();
            Iterator<Record> actualIter = dao.iterator(from, to);

            assertEquals(calcIteratorSize(expectedIter), calcIteratorSize(actualIter));
        }
    }

    @Test
    void iteration() throws IOException {
        try (DAO dao = createDao()) {
            int count = 100;
            NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                map.put(key, value);
            }

            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> expectedIter = map.entrySet().iterator();
            Iterator<Record> actualIter = dao.iterator();

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
    void fromIteration() throws IOException {
        try (DAO dao = createDao()) {
            int count = 100;
            NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                map.put(key, value);
            }

            int fromIndex = new Random().nextInt(count / 2);
            ByteBuffer from = map.entrySet().stream().skip(fromIndex).findFirst().get().getKey();

            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> expectedIter = map.tailMap(from).entrySet().iterator();
            Iterator<Record> actualIter = dao.iterator(from);
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
    void rangeIteration() throws IOException {
        try (DAO dao = createDao()) {
            int count = 100;
            NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                ByteBuffer key = randomKey();
                ByteBuffer value = randomValue();
                dao.upsert(key, value);
                map.put(key, value);
            }

            int fromIndex = new Random().nextInt(count / 2);
            int toIndex = fromIndex + new Random().nextInt(count / 2);
            ByteBuffer from = map.entrySet().stream().skip(fromIndex).findFirst().get().getKey();
            ByteBuffer to = map.entrySet().stream().skip(toIndex).findFirst().get().getKey();

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
