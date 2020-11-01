package com.accakyra.lsss.lsm.storage;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class Index {

    private final NavigableMap<ByteBuffer, KeyInfo> keys;

    public Index(NavigableMap<ByteBuffer, KeyInfo> keyInfos) {
        this.keys = keyInfos;
    }

    public KeyInfo getKeyInfo(ByteBuffer key) {
        return keys.get(key);
    }

    public NavigableSet<ByteBuffer> keys() {
        return keys.navigableKeySet();
    }
}
