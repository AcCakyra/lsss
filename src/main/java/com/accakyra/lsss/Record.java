package com.accakyra.lsss;

import java.nio.ByteBuffer;

public class Record implements Comparable<Record> {

    private final ByteBuffer key;
    private final ByteBuffer value;

    public Record(ByteBuffer key, ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public int compareTo(Record o) {
        return this.key.compareTo(o.key);
    }
}
