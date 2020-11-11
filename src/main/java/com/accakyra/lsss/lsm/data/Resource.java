package com.accakyra.lsss.lsm.data;

import com.accakyra.lsss.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Resource extends Iterable<Record> {

    Record get(ByteBuffer key);

    Iterator<Record> iterator(ByteBuffer from);

    Iterator<Record> iterator(ByteBuffer from, ByteBuffer to);
}
