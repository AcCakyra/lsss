package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;

public class Index {

    private final NavigableSet<Key> keys;
    private final int generation;

    public Index(NavigableSet<Key> keys, int generation) {
        this.keys = keys;
        this.generation = generation;
    }

    public int calcIndexSize() {
        return keys.stream()
                .mapToInt(Key::calcSize)
                .sum();
    }

    public Key getKey(ByteBuffer key) {
        return keys.stream().filter(k -> k.getKey().equals(key)).findFirst().orElse(null);
    }

    public Iterator<Key> getIterator() {
        return keys.iterator();
    }

    public int getGeneration() {
        return generation;
    }
}
