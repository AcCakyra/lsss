package com.accakyra.lsss.lsm.data.persistent.io.write;

import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.io.FileWriter;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.nio.file.Path;

public class TableWriter {

    public static void writeTable(Table table, Path storagePath) {
        Path sstableFileName = FileNameUtil.buildSSTableFileName(storagePath, table.getId());
        Path indexFileName = FileNameUtil.buildIndexFileName(storagePath, table.getId());
        FileWriter.write(indexFileName, table.getIndexBuffer());
        FileWriter.write(sstableFileName, table.getSstBuffer());
    }
}
