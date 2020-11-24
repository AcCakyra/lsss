package com.accakyra.lsss.lsm.data.persistent.io.write;

import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.io.FileWriter;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.nio.file.Path;

public class TableWriter {

    public static void writeTable(Table table, int tableId, Path storagePath) {
        Path sstableFileName = FileNameUtil.buildSSTableFileName(storagePath, tableId);
        Path indexFileName = FileNameUtil.buildIndexFileName(storagePath, tableId);
        FileWriter.write(indexFileName, table.getIndexBuffer());
        FileWriter.write(sstableFileName, table.getSstBuffer());
    }
}
