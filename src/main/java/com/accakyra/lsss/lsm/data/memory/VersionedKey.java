package com.accakyra.lsss.lsm.data.memory;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
@AllArgsConstructor
public class VersionedKey implements Comparable<VersionedKey> {

    private final ByteBuffer key;
    private final int version;

    @Override
    public int compareTo(VersionedKey o) {
        int compare = key.compareTo(o.getKey());
        if (compare == 0) {
            return o.getVersion() - version;
        } else {
            return compare;
        }
    }
}