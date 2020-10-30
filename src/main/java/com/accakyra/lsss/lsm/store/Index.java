package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class Index {

    private final NavigableMap<ByteBuffer, KeyInfo> keys;
    private final int generation;

    public Index(NavigableMap<ByteBuffer, KeyInfo> keyInfos, int generation) {
        this.keys = keyInfos;
        this.generation = generation;
    }

    public KeyInfo getKeyInfo(ByteBuffer key) {
        return keys.get(key);
    }

    public NavigableSet<ByteBuffer> keys() {
        return keys.navigableKeySet();
    }

    public int getGeneration() {
        return generation;
    }
}
