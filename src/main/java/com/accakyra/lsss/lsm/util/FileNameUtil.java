package com.accakyra.lsss.lsm.util;

public class FileNameUtil {

    public static String buildSstableFileName(String folderName, int generation) {
        return folderName + "/" + "sstable" + generation + ".sst";
    }

    public static String buildIndexFileName(String folderName, int generation) {
        return folderName + "/" + "index" + generation + ".idx";
    }

    public static String buildMetaDataFileName(String folderName) {
        return folderName + "/" + "metadata" + ".mx";
    }
}
