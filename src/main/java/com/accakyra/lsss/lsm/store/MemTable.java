package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.*;

public class MemTable {

    private final NavigableMap<ByteBuffer, ByteBuffer> memtable;
    private int keysCapacity;

    public MemTable() {
        this.memtable = new TreeMap<>();
    }

    public ByteBuffer get(ByteBuffer key) {
        return memtable.get(key);
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        if (!memtable.containsKey(key)) {
            keysCapacity += key.capacity();
        }
        memtable.put(key, value);
    }

    public int getKeysCapacity() {
        return keysCapacity;
    }

    public int getSize() {
        return memtable.size();
    }

    public Iterator<Map.Entry<ByteBuffer, ByteBuffer>> getIterator() {
        return memtable.entrySet().iterator();
    }
}
