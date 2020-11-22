package com.accakyra.lsss.lsm.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileRemover {

    public static void remove(Path file) {
        try {
            Files.delete(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
