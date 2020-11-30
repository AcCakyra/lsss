package com.accakyra.lsss;

import com.accakyra.lsss.lsm.LSMTree;

import java.io.File;

public class DAOFactory {

    public static DAO create(File data) {
        return create(data, Config.builder().build());
    }

    public static DAO create(File data, Config config) {
        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }
        if (!data.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + data);
        }
        return new LSMTree(data, config);
    }
}
