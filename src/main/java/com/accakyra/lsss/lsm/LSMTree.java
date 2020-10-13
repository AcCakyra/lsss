package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LSMTree implements DAO {
    private final ConcurrentNavigableMap<ByteBuffer, ByteBuffer> memtable;

    public LSMTree() {
        memtable = new ConcurrentSkipListMap<>();
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        memtable.put(key, value);
    }

    public ByteBuffer get(ByteBuffer key) {
        ByteBuffer value = memtable.get(key);
        if (value == null) {
            throw new NoSuchElementException();
        }
        return value;
    }

    public void delete(ByteBuffer key) {
        memtable.remove(key);
    }

    public void close() throws IOException {

    }
}
