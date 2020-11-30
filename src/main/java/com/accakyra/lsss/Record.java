package com.accakyra.lsss;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
@AllArgsConstructor
public class Record implements Comparable<Record> {

    /**
     * Special value for deleted keys.
     */
    public static final ByteBuffer TOMBSTONE = ByteBuffer.wrap("TTTT".getBytes());
    private final ByteBuffer key;
    private final ByteBuffer value;

    @Override
    public int compareTo(Record o) {
        return getKey().compareTo(o.getKey());
    }
}
