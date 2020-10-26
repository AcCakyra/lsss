package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;
import java.util.Map;

public class Index {

    private final Map<ByteBuffer, KeyInfo> keys;
    private final int generation;

    public Index(Map<ByteBuffer, KeyInfo> keyInfos, int generation) {
        this.keys = keyInfos;
        this.generation = generation;
    }

    public KeyInfo getKey(ByteBuffer key) {
        return keys.get(key);
    }

    public int getGeneration() {
        return generation;
    }
}
