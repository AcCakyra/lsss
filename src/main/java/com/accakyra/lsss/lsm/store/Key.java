package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;

public class Key {

    private final int size;
    private final ByteBuffer key;
    private final int offset;
    private final int valueSize;

    public Key(int size, ByteBuffer key, int offset, int valueSize) {
        this.size = size;
        this.key = key;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public int getSize() {
        return size;
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
}
