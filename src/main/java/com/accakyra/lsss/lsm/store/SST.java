package com.accakyra.lsss.lsm.store;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.TableReader;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class SST implements Resource {

    private final Index index;
    private final TableReader reader;

    public SST(Index index, TableReader reader) {
        this.index = index;
        this.reader = reader;
    }

    @Override
    public Record get(ByteBuffer key) {
        KeyInfo keyInfo = index.getKeyInfo(key);
        if (keyInfo == null) return null;
        ByteBuffer value = reader.getValueFromStorage(keyInfo, index.getGeneration());
        return new Record(key, value);
    }

    @Override
    public Iterator<Record> iterator() {
        return index.keys()
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = reader.getValueFromStorage(keyInfo, index.getGeneration());
                    return new Record(key, value);
                })
                .iterator();
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        return index.keys()
                .tailSet(from)
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = reader.getValueFromStorage(keyInfo, index.getGeneration());
                    return new Record(key, value);
                })
                .iterator();
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        return index.keys()
                .subSet(from, to)
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = reader.getValueFromStorage(keyInfo, index.getGeneration());
                    return new Record(key, value);
                })
                .iterator();
    }
}
