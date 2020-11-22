package com.accakyra.lsss;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface DAO extends Closeable {

    void upsert(ByteBuffer key, ByteBuffer value);

    ByteBuffer get(ByteBuffer key);

    void delete(ByteBuffer key);

    CloseableIterator<Record> iterator();

    CloseableIterator<Record> iterator(ByteBuffer from);

    CloseableIterator<Record> iterator(ByteBuffer from, ByteBuffer to);
}
