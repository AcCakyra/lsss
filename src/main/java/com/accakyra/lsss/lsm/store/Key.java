package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;

public class Key implements Comparable<Key> {

    private final ByteBuffer key;
    private final int snapshot;

    public Key(ByteBuffer key, int snapshot) {
        this.key = key;
        this.snapshot = snapshot;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public int getSnapshot() {
        return snapshot;
    }

    @Override
    public int compareTo(Key o) {
        int compare = key.compareTo(o.getKey());
        if (compare == 0) {
            return snapshot - o.getSnapshot();
        } else {
            return compare;
        }
    }
}
