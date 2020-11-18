package com.accakyra.lsss.lsm.util.iterators;

import com.accakyra.lsss.Record;

import java.util.Iterator;
import java.util.List;

public class Iterators {

    public static <T extends Comparable<T>> MergedIterator<T> mergeIterator(List<Iterator<T>> iterators) {
        return new MergedIterator<>(iterators);
    }

    public static <T extends Comparable<T>> DistinctIterator<T> distinctIterator(Iterator<T> iterator) {
        return new DistinctIterator<>(iterator);
    }

    public static RemoveTombstonesIterator removeTombstonesIterator(Iterator<Record> iterator) {
        return new RemoveTombstonesIterator(iterator);
    }
}
