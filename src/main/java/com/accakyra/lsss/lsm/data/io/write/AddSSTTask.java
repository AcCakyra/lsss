package com.accakyra.lsss.lsm.data.io.write;

import com.accakyra.lsss.lsm.data.persistent.Level;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.data.io.Table;
import com.accakyra.lsss.lsm.io.FileWriter;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;

public class AddSSTTask implements Runnable {

    private final Table table;
    private final Path storagePath;
    private final SST sst;
    private final Lock lock;
    private final Level level;

    public AddSSTTask(Table table, Path storagePath, SST sst, Lock lock, Level level) {
        this.table = table;
        this.storagePath = storagePath;
        this.sst = sst;
        this.lock = lock;
        this.level = level;
    }

    @Override
    public void run() {
        Path sstableFileName = FileNameUtil.buildSstableFileName(storagePath, table.getId());
        Path indexFileName = FileNameUtil.buildIndexFileName(storagePath, table.getId());
        FileWriter.write(indexFileName, table.getIndexBuffer());
        FileWriter.write(sstableFileName, table.getSstBuffer());

        lock.lock();
        level.add(sst);
        lock.unlock();
    }
}
