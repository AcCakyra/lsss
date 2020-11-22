package com.accakyra.lsss.lsm.util.iterators;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.CloseableIterator;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;

public class IteratorsUtil {

    public static <T extends Comparable<T>> MergedIterator<T> mergeIterator(List<Iterator<T>> iterators) {
        return new MergedIterator<>(iterators);
    }

    public static <T extends Comparable<T>> DistinctIterator<T> distinctIterator(Iterator<T> iterator) {
        return new DistinctIterator<>(iterator);
    }

    public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator, AutoCloseable closeable) {
        return new CloseableIteratorImpl<>(iterator, closeable);
    }

    public static Iterator<Record> removeTombstonesIterator(Iterator<Record> iterator) {
        return Iterators.filter(iterator, (record) -> !record.getValue().equals(Record.TOMBSTONE));
    }
}
