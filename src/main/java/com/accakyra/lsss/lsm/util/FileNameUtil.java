package com.accakyra.lsss.lsm.util;

import java.nio.file.Path;
import java.util.regex.Pattern;

public class FileNameUtil {

    private static Pattern indexPattern = Pattern.compile("index\\d+.idx");

    public static Path buildSSTableFileName(Path folderName, int id) {
        return Path.of(folderName + "/" + "sstable" + id + ".sst");
    }

    public static Path buildIndexFileName(Path folderName, int id) {
        return Path.of(folderName + "/" + "index" + id + ".idx");
    }

    public static boolean isIndexFileName(String fileName) {
        return indexPattern.asMatchPredicate().test(fileName);
    }

    public static int extractIdFormIndexFileName(String fileName) {
        return Integer.parseInt(fileName.replaceAll("[^0-9]", ""));
    }

    public static Path buildMetaDataFileName(Path folderName) {
        return Path.of(folderName + "/" + "metadata" + ".mx");
    }
}
