package com.accakyra.lsss;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MemTableTest extends TestBase {
    @Test
    public void empty() throws IOException {
        try (DAO dao = DAOFactory.create()) {
            assertThrows(NoSuchElementException.class, () -> dao.get(randomKey()));
        }
    }

    @Test
    public void insert() throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();
        try (DAO dao = DAOFactory.create()) {
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
        try (DAO dao = DAOFactory.create()) {
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
        try (DAO dao = DAOFactory.create()) {
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
        try (DAO dao = DAOFactory.create()) {
            dao.delete(key);
        }
    }
}
