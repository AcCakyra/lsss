package com.accakyra.lsss;

import com.google.common.collect.Iterators;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class IterationTest extends TestBase {

    @Test
    void emptyIterator() throws IOException {
        try (DAO dao = createDao()) {
            ByteBuffer from = randomKey();
            ByteBuffer to = randomKey();
            Iterators.elementsEqual(Collections.emptyIterator(), dao.iterator(from, to));
        }
    }

    @Test
    void iteration() throws Exception {
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
            try (CloseableIterator<Record> actualIter = dao.iterator()) {
                Iterators.elementsEqual(expectedIter, actualIter);
            }
        }
    }

    @Test
    void IterationFrom() throws Exception {
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
            try (CloseableIterator<Record> actualIter = dao.iterator(from)) {
                Iterators.elementsEqual(expectedIter, actualIter);
            }
        }
    }

    @Test
    void iterationFromTo() throws Exception {
        try (DAO dao = createDao()) {
            int count = 100;
            TreeMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
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
            try (CloseableIterator<Record> actualIter = dao.iterator(from, to)) {
                Iterators.elementsEqual(expectedIter, actualIter);
            }
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

            assertEquals(9, Iterators.size(dao.iterator()));

            assertEquals(4, Iterators.size(dao.iterator(stringToByteBuffer("BBB2"))));
            assertEquals(5, Iterators.size(dao.iterator(stringToByteBuffer("AAA5"))));

            assertEquals(9, Iterators.size(dao.iterator(
                    stringToByteBuffer("A"),
                    stringToByteBuffer("ZZZZZ")))
            );
            assertEquals(1, Iterators.size(dao.iterator(
                    stringToByteBuffer("AAA3"),
                    stringToByteBuffer("AAA4")))
            );
            assertEquals(1, Iterators.size(dao.iterator(
                    stringToByteBuffer("BBB3"),
                    stringToByteBuffer("CCC2")))
            );
        }
    }

    @Test
    void iterationAfterClose() throws Exception {
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
            try (CloseableIterator<Record> actualIter = dao.iterator(from, to)) {
                Iterators.elementsEqual(expectedIter, actualIter);
            }
        }
    }

    @Test
    void iterationAndInsert() throws Exception {
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
            try (CloseableIterator<Record> actualIter = dao.iterator()) {

                Iterators.elementsEqual(
                        Iterators.limit(expectedIter, 500),
                        Iterators.limit(actualIter, 500));

                for (int i = 0; i < 1000; i++) {
                    dao.upsert(randomKey(), randomValue());
                }

                Iterators.elementsEqual(expectedIter, actualIter);
            }
        }
    }

    @Test
    void daoAfterClose() throws IOException {
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
            assertEquals(9, Iterators.size(dao.iterator()));

            assertEquals(4, Iterators.size(dao.iterator(stringToByteBuffer("BBB2"))));
            assertEquals(5, Iterators.size(dao.iterator(stringToByteBuffer("AAA5"))));

            assertEquals(9, Iterators.size(dao.iterator(
                    stringToByteBuffer("A"),
                    stringToByteBuffer("ZZZZZ")))
            );
            assertEquals(1, Iterators.size(dao.iterator(
                    stringToByteBuffer("AAA3"),
                    stringToByteBuffer("AAA4")))
            );
            assertEquals(1, Iterators.size(dao.iterator(
                    stringToByteBuffer("BBB3"),
                    stringToByteBuffer("CCC2")))
            );
        }
    }
}
