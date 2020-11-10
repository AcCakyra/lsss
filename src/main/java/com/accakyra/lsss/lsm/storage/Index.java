package com.accakyra.lsss.lsm.storage;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class Index {

    private final int level;
    private final NavigableMap<ByteBuffer, KeyInfo> keys;

    public Index(int level, NavigableMap<ByteBuffer, KeyInfo> keyInfos) {
        this.level = level;
        this.keys = keyInfos;
    }

    public KeyInfo getKeyInfo(ByteBuffer key) {
        return keys.get(key);
    }

    public NavigableSet<ByteBuffer> keys() {
        return keys.navigableKeySet();
    }

    public int getLevel() {
        return level;
    }
}
