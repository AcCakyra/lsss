package com.accakyra.lsss;

import com.accakyra.lsss.lsm.LSMTree;

public class DAOFactory {
    public static DAO create() {
        return new LSMTree();
    }
}
