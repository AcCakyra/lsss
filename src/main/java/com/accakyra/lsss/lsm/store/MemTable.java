package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

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
