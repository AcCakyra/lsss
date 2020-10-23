package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;

public class Key implements Comparable<Key> {

    private int keySize;
    private final ByteBuffer key;
    private int offset;
    private int valueSize;

    public Key(ByteBuffer key) {
        this.key = key;
    }

    public Key(int keySize, ByteBuffer key, int offset, int valueSize) {
        this.keySize = keySize;
        this.key = key;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    /**
     * Calc size of {@link Key} in bytes
     */
    public int calcSize() {
        return 4 + getKeySize() + 4 + 4;
    }

    public int getKeySize() {
        return keySize;
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
