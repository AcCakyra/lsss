package com.accakyra.lsss.lsm.io.write;

import com.accakyra.lsss.lsm.data.persistent.sst.Index;
import com.accakyra.lsss.lsm.data.persistent.Level;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.data.persistent.sst.Table;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteSSTableTask implements Runnable {

    private final Table table;
    private final int id;
    private final Path folderPath;
    private final Level level;
    private final Index index;
    private final ReentrantReadWriteLock lock;

    public WriteSSTableTask(Table table, int id, Path folderPath, Level level,
                            Index index, ReentrantReadWriteLock lock) {
        this.table = table;
        this.id = id;
        this.folderPath = folderPath;
        this.level = level;
        this.index = index;
        this.lock = lock;
    }

    @Override
    public void run() {
        Path sstableFileName = FileNameUtil.buildSstableFileName(folderPath, id);
        Path indexFileName = FileNameUtil.buildIndexFileName(folderPath, id);
        FileWriter.write(indexFileName, table.getIndexBuffer());
        FileWriter.write(sstableFileName, table.getSstBuffer());

        SST sst = new SST(index, id, sstableFileName);
        lock.writeLock().lock();
        level.add(sst);
        lock.writeLock().unlock();
    }

}
