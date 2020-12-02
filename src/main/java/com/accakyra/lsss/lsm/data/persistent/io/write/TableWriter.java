package com.accakyra.lsss.lsm.data.persistent.io.write;

import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.io.FileWriter;
import com.accakyra.lsss.lsm.util.FileNameUtil;
import lombok.extern.java.Log;

import java.io.IOException;
import java.nio.file.Path;

@Log
public class TableWriter {

    public static void writeTable(Table table, int tableId, Path storagePath) {
        try {
            Path sstableFileName = FileNameUtil.buildSSTableFileName(storagePath, tableId);
            Path indexFileName = FileNameUtil.buildIndexFileName(storagePath, tableId);
            FileWriter.write(indexFileName, table.getIndexBuffer());
            FileWriter.write(sstableFileName, table.getSstBuffer());
        } catch (IOException e) {
            log.log(java.util.logging.Level.SEVERE,
                    "Cannot write table with id : " + tableId + " to " + storagePath.toString(), e);
        }
    }
}
