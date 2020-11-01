package com.accakyra.lsss.lsm;

import com.accakyra.lsss.lsm.storage.Memtable;
import com.accakyra.lsss.lsm.storage.Resource;
import com.accakyra.lsss.lsm.storage.SST;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.List;

public class SSTables implements Iterable<Resource>, Closeable {

    private List<Resource> ssts;
    private TableManager tableManager;

    public SSTables(File data) {
        tableManager = new TableManager(data);
        ssts = tableManager.readSSTs();
    }

    public void addMemtable(Memtable memtable) {
        SST sst = tableManager.writeMemtable(memtable);
        ssts.add(sst);
    }

    @Override
    public Iterator<Resource> iterator() {
        return ssts.iterator();
    }

    @Override
    public void close() {
        tableManager.close();
    }
}

