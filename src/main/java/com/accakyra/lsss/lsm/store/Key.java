package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;

public class Key implements Comparable<Key> {

    private final ByteBuffer key;
    private int offset;
    private int valueSize;

    public Key(ByteBuffer key, int offset, int valueSize) {
        this.key = key;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

    public int getValueSize() {
        return valueSize;
    }

    @Override
    public int compareTo(Key o) {
        return this.key.compareTo(o.key);
    }
}
