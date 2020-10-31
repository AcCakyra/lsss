package com.accakyra.lsss;

import com.accakyra.lsss.lsm.Record;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface DAO extends Iterable<Record>, Closeable {

    void upsert(ByteBuffer key, ByteBuffer value);

    ByteBuffer get(ByteBuffer key);

    void delete(ByteBuffer key);

    Iterator<Record> iterator(ByteBuffer from);

    Iterator<Record> iterator(ByteBuffer from, ByteBuffer to);
}
