package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.*;

public class MemTable {

    private final NavigableMap<ByteBuffer, ByteBuffer> memtable;

    public MemTable() {
        this.memtable = new TreeMap<>();
    }

    public ByteBuffer get(ByteBuffer key) {
        return memtable.get(key);
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        memtable.put(key, value);
    }

    public Iterator<Map.Entry<ByteBuffer, ByteBuffer>> getIterator() {
        return memtable.entrySet().iterator();
    }
}
