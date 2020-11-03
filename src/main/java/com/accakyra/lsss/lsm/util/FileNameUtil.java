package com.accakyra.lsss.lsm.util;

import java.nio.file.Path;

public class FileNameUtil {

    public static Path buildSstableFileName(Path folderName, int generation) {
        return Path.of(folderName + "/" + "sstable" + generation + ".sst");
    }

    public static Path buildIndexFileName(Path folderName, int generation) {
        return Path.of(folderName + "/" + "index" + generation + ".idx");
    }

    public static Path buildMetaDataFileName(Path folderName) {
        return Path.of(folderName + "/" + "metadata" + ".mx");
    }
}
