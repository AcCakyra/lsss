package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.*;

public class MemTable {

    private final NavigableMap<Key, ByteBuffer> memtable;
    private int offset;

    public MemTable() {
        this.memtable = new TreeMap<>();
    }

    public ByteBuffer get(ByteBuffer key) {
        Key getKey = new Key(key);
        return memtable.get(getKey);
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        int keySize = key.remaining();
        int valueSize = value.remaining();

        offset += keySize;
        Key infoKey = new Key(keySize, key, offset, valueSize);
        offset += valueSize;

        memtable.put(infoKey, value);
    }

    public int calcKeysSize() {
        return memtable.keySet()
                .stream()
                .mapToInt(Key::calcSize)
                .reduce(Integer::sum)
                .orElse(0);
    }

    public Iterator<Map.Entry<Key, ByteBuffer>> getIterator() {
        return memtable.entrySet().iterator();
    }
}
