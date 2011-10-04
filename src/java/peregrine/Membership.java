package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Membership {

    private Map<Partition,List<Host>> delegate = new HashMap();

    public Set<Partition> getPartitions() {
        return delegate.keySet();
    }

    public List<Host> getHosts( Partition part ) {
        return delegate.get( part );
    }
    
}

