package com.accakyra.lsss.lsm.data;

import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.util.Collection;

public interface Run extends Resource {

    void add(SST sst);

    Collection<SST> getSstables();

    int size();
}