package com.accakyra.lsss.lsm.data.memory;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;
import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.*;

public class Memtable implements Resource {

    private final NavigableMap<VersionedKey, ByteBuffer> memtable;
    private int snapshot;
    private int keysSize;
    private int valuesSize;
    private int uniqueKeysCount;

    public Memtable() {
        this.memtable = new TreeMap<>();
    }

    @Override
    public Record get(ByteBuffer key) {
        VersionedKey keyToFind = new VersionedKey(key, snapshot);
        VersionedKey recentKey = memtable.ceilingKey(keyToFind);
        if (recentKey != null && recentKey.getKey().equals(key)) {
            return get(recentKey);
        }
        return null;
    }

    public Record get(VersionedKey key) {
        ByteBuffer value = memtable.get(key);
        if (value != null) {
            return new Record(key.getKey(), value);
        }
        return null;
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        if (get(key) == null) {
            uniqueKeysCount++;
            keysSize += key.limit();
        }
        valuesSize += value.limit();
        VersionedKey versionedKey = new VersionedKey(key, snapshot++);
        memtable.put(versionedKey, value);
    }

    public boolean isEmpty() {
        return getUniqueKeysCount() == 0;
    }

    public int getSize() {
        return keysSize + valuesSize;
    }

    public int getKeysSize() {
        return keysSize;
    }

    public int getUniqueKeysCount() {
        return uniqueKeysCount;
    }

    @Override
    public Iterator<Record> iterator() {
        if (isEmpty()) return Collections.emptyIterator();
        return iterator(memtable.firstKey().getKey());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        return iterator(from, null);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        VersionedKey fromKey = new VersionedKey(from, snapshot);
        VersionedKey toKey = null;
        if (to != null) toKey = new VersionedKey(to, snapshot);

        Iterator<VersionedKey> navigableIterator =
                IteratorsUtil.navigableIterator(memtable.navigableKeySet(), fromKey, toKey);
        Iterator<Record> iterator = Iterators.transform(navigableIterator, this::get);
        return IteratorsUtil.distinctIterator(iterator);
    }
}
